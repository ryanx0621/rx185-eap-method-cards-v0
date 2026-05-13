/// Telegram long-poll loop implementing spec T1-T8 + dispatch state machine (T2.2).
///
/// T1   Single-writer lease (TTL 15 s, refresh every 5 s).
/// T2   Offset written to DB only AFTER all rows are durably ingested.
/// T2.2 Dispatch is decoupled from ingest. `poll_once` writes raw rows as
///      `pending_dispatch`; `dispatch_pending` later moves them to
///      `dispatched` atomically with the outbox enqueue (single SQLite tx).
///      A crash between ingest and dispatch is recoverable on next tick.
/// T3   On boot: getWebhookInfo -> deleteWebhook if a URL is set.
/// T4   getUpdates timeout=25, HTTP read timeout=35 s (set on ReqwestClient).
/// T5   Backoff ladder [1,2,4,8,16,32]s; skip 401/404; honour 429 retry_after.
/// T6   update_id uniqueness via INSERT OR IGNORE into telegram_updates.
/// T7   Outbox rate limiting (delegated to OutboxManager).
/// T8   getMe + getWebhookInfo health probe every 30 s.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use rusqlite::params;
use uuid::Uuid;

use legion_bus::{BusDb, leases::LeaseManager};
use legion_types::lease::LeaseScope;

use crate::api::TelegramApi;
use crate::error::{TgError, TgResult};
use crate::offset::{load_offset, record_update, save_offset};
use crate::outbox::OutboxManager;
use crate::types::Update;

// --- Configuration ----------------------------------------------------------

pub struct PollerConfig {
    pub bot_id: String,
    /// T4: passed as getUpdates `timeout` param (seconds).
    pub long_poll_timeout_secs: u64,
    /// T1: lease TTL.
    pub lease_ttl_secs: u64,
    /// T1: how often to renew the lease within the loop.
    pub lease_refresh_secs: u64,
    /// T8: health probe interval.
    pub health_probe_interval_secs: u64,
    /// T5: maximum number of consecutive error retries.
    pub max_retries: u32,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            bot_id: "default".into(),
            long_poll_timeout_secs: 25,
            lease_ttl_secs: 15,
            lease_refresh_secs: 5,
            health_probe_interval_secs: 30,
            max_retries: 10,
        }
    }
}

// --- Poller -----------------------------------------------------------------

pub struct Poller<A: TelegramApi> {
    api: Arc<A>,
    db: Arc<Mutex<BusDb>>,
    outbox: Arc<OutboxManager>,
    config: PollerConfig,
}

impl<A: TelegramApi> Poller<A> {
    pub fn new(
        api: Arc<A>,
        db: Arc<Mutex<BusDb>>,
        outbox: Arc<OutboxManager>,
        config: PollerConfig,
    ) -> Self {
        Self { api, db, outbox, config }
    }

    /// T1 + T3: acquire single-writer lease, delete webhook if present.
    ///
    /// Returns the acquired lease_id on success.
    pub async fn boot(&self) -> TgResult<Uuid> {
        // T1: acquire exclusive poller lease.
        let lease_id = {
            let db = self.db.lock().unwrap();
            let mgr = LeaseManager::new(&db);
            mgr.acquire(
                LeaseScope::TelegramPoller,
                &self.config.bot_id,
                self.config.lease_ttl_secs,
            )?
        };
        tracing::info!(bot_id = %self.config.bot_id, %lease_id, "poller lease acquired");

        // T3: reconcile webhook -- if one is set, delete it so polling works cleanly.
        let info = self.api.get_webhook_info().await?;
        if !info.url.is_empty() {
            tracing::warn!(
                url = %info.url,
                "active webhook found -- deleting before polling (T3)"
            );
            self.api.delete_webhook().await?;
        }

        Ok(lease_id)
    }

    /// Ingest one batch of updates (T2 + T6).
    ///
    /// Pure ingest path. Updates are recorded as `pending_dispatch`. Dispatch
    /// is the job of `dispatch_pending`, called separately.
    ///
    /// Order (all crash-safe under SQLite WAL):
    ///   1. load offset
    ///   2. get_updates
    ///   3. INSERT each row (status='pending_dispatch'); T6 deduplicates replays
    ///   4. save offset = max_id + 1 (only after every row is durable)
    ///
    /// Crash between 3 and 4: Telegram resends the same batch, INSERT OR IGNORE
    /// returns 0 changes for already-recorded rows. The rows remain
    /// `pending_dispatch` and will be picked up by `dispatch_pending`.
    pub async fn poll_once(&self) -> TgResult<usize> {
        let offset = load_offset(&self.db, &self.config.bot_id)?;
        let updates = self
            .api
            .get_updates(offset, self.config.long_poll_timeout_secs, 100)
            .await?;

        if updates.is_empty() {
            return Ok(0);
        }

        let max_id = updates.iter().map(|u| u.update_id).max().unwrap_or(offset);

        let mut new_count = 0usize;
        for update in &updates {
            let raw = serde_json::to_string(update).unwrap_or_default();
            let is_new = record_update(&self.db, update.update_id, &raw)?;
            if is_new {
                new_count += 1;
            } else {
                tracing::debug!(update_id = update.update_id, "duplicate update_id ignored (T6)");
            }
        }

        // T2: offset advances only AFTER every row is durably ingested.
        save_offset(&self.db, &self.config.bot_id, max_id + 1)?;

        Ok(new_count)
    }

    /// Drain `pending_dispatch` rows from `telegram_updates` (T2.2).
    ///
    /// Per-row contract — entirely DB-side, no Telegram API calls here:
    ///   BEGIN
    ///   3. INSERT outbox row (if message is a /command)
    ///   4. UPDATE telegram_updates SET status='dispatched' WHERE status='pending_dispatch'
    ///      - if rows_changed == 0, another worker (or earlier retry) already won; ROLLBACK
    ///   5. COMMIT
    ///
    /// If anything between BEGIN and COMMIT fails, the tx auto-rollbacks on
    /// drop — both writes are undone, the row stays `pending_dispatch`, and
    /// the next call picks it up again. Exactly-once is guaranteed by the
    /// WHERE status='pending_dispatch' guard inside step 4.
    pub fn dispatch_pending(&self) -> TgResult<usize> {
        // 1. Pull a batch of candidate rows.
        let pending: Vec<(i64, String)> = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.conn.prepare(
                "SELECT update_id, raw_json FROM telegram_updates
                  WHERE status = 'pending_dispatch'
                  ORDER BY update_id
                  LIMIT 50",
            )?;
            let rows: Vec<(i64, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .flatten()
                .collect();
            rows
        };

        let mut dispatched = 0usize;
        for (update_id, raw_json) in pending {
            // 2. Parse outside any transaction (pure CPU, no DB).
            let update: Update = match serde_json::from_str(&raw_json) {
                Ok(u) => u,
                Err(e) => {
                    self.mark_rejected(update_id, &format!("parse: {e}"))?;
                    continue;
                }
            };

            // Render the response under the lock (commands::handle is read-only).
            let response = {
                let db = self.db.lock().unwrap();
                render_response(&update, &db)
            };

            // 3+4+5: atomic enqueue + status transition.
            let db = self.db.lock().unwrap();
            let tx = db.conn.unchecked_transaction()?;

            // 3. Enqueue outbox row (only if there's a /command response).
            if let Some((chat_id, text)) = response.as_ref() {
                let seq: i64 = tx
                    .query_row(
                        "SELECT COALESCE(MAX(message_seq), 0) + 1 FROM telegram_outbox WHERE chat_id = ?1",
                        params![chat_id],
                        |r| r.get(0),
                    )
                    .unwrap_or(1);
                let payload = serde_json::json!({ "text": text }).to_string();
                tx.execute(
                    "INSERT INTO telegram_outbox
                        (chat_id, message_seq, payload, method, status, retry_count, created_at)
                     VALUES (?1, ?2, ?3, 'sendMessage', 'pending', 0, ?4)",
                    params![chat_id, seq, payload, Utc::now().to_rfc3339()],
                )?;
            }

            // 4. Mark dispatched; the WHERE status='pending_dispatch' guard
            //    is the exactly-once gate against a racing worker or a
            //    retried previous tx that already committed.
            let claimed = tx.execute(
                "UPDATE telegram_updates
                    SET status='dispatched', dispatched_at=?1
                  WHERE update_id=?2 AND status='pending_dispatch'",
                params![Utc::now().to_rfc3339(), update_id],
            )?;

            if claimed == 0 {
                // Race lost (someone else won this row, or it was rejected).
                // Drop the tx without committing -> rollback the outbox INSERT.
                drop(tx);
                tracing::debug!(update_id, "dispatch race lost; tx rolled back");
                continue;
            }

            // 5. Commit.
            tx.commit()?;
            dispatched += 1;
        }

        Ok(dispatched)
    }

    fn mark_rejected(&self, update_id: i64, reason: &str) -> TgResult<()> {
        let db = self.db.lock().unwrap();
        db.conn.execute(
            "UPDATE telegram_updates
                SET status='rejected', dispatch_error=?1, dispatched_at=?2
              WHERE update_id=?3 AND status='pending_dispatch'",
            params![reason, Utc::now().to_rfc3339(), update_id],
        )?;
        Ok(())
    }

    /// Main polling loop -- runs until a hard error (401/404/LeaseLost) or task cancel.
    pub async fn run(&self, lease_id: Uuid) -> TgResult<()> {
        let mut consecutive_errors: u32 = 0;
        let mut last_health = Instant::now();
        let mut last_lease_refresh = Instant::now();

        loop {
            // T8: health probe every 30 s.
            if last_health.elapsed()
                >= Duration::from_secs(self.config.health_probe_interval_secs)
            {
                if let Err(e) = self.health_probe().await {
                    tracing::warn!(?e, "health probe failed (T8)");
                }
                last_health = Instant::now();
            }

            // T1: renew lease every 5 s.
            if last_lease_refresh.elapsed()
                >= Duration::from_secs(self.config.lease_refresh_secs)
            {
                let renewed = {
                    let db = self.db.lock().unwrap();
                    LeaseManager::new(&db).renew(lease_id, &self.config.bot_id)
                };
                match renewed {
                    Ok(_) => last_lease_refresh = Instant::now(),
                    Err(e) => {
                        tracing::error!(?e, "lease renewal failed -- stopping poller (T1)");
                        return Err(TgError::LeaseLost);
                    }
                }
            }

            match self.poll_once().await {
                Ok(n) => {
                    consecutive_errors = 0;
                    tracing::debug!(new_updates = n, "poll_once done");

                    // T2.2: drain pending dispatches. Includes the rows we just
                    // ingested AND any rows recovered from a prior crash.
                    match self.dispatch_pending() {
                        Ok(d) if d > 0 => tracing::debug!(dispatched = d, "dispatch_pending drained"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(?e, "dispatch_pending failed; rows remain recoverable"),
                    }
                }
                // T5: never retry these.
                Err(TgError::Unauthorized) | Err(TgError::NotFound) => {
                    tracing::error!("fatal Telegram auth error -- stopping poller");
                    return Err(TgError::Unauthorized);
                }
                Err(TgError::LeaseLost) => return Err(TgError::LeaseLost),
                // T3: 409 means a webhook is active alongside polling; delete it and continue.
                Err(TgError::Conflict) => {
                    tracing::warn!("409 Conflict detected -- deleting webhook to restore polling (T3)");
                    consecutive_errors = 0;
                    if let Err(e) = self.api.delete_webhook().await {
                        tracing::error!(?e, "deleteWebhook reconcile failed after 409 -- stopping poller");
                        return Err(TgError::Conflict);
                    }
                }
                // T5: honour 429 retry_after.
                Err(TgError::RateLimited { retry_after_secs }) => {
                    tracing::warn!(retry_after_secs, "rate limited (429), backing off (T5)");
                    tokio::time::sleep(Duration::from_secs(retry_after_secs)).await;
                }
                // T5: exponential backoff for other errors.
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors > self.config.max_retries {
                        tracing::error!(
                            ?e,
                            consecutive_errors,
                            "max retries exceeded -- stopping poller"
                        );
                        return Err(e);
                    }
                    let backoff_secs = backoff_secs(consecutive_errors);
                    tracing::warn!(
                        ?e,
                        attempt = consecutive_errors,
                        backoff_secs,
                        "transient error, retrying (T5)"
                    );
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                }
            }
        }
    }

    /// T8: getMe + getWebhookInfo health probe.
    async fn health_probe(&self) -> TgResult<()> {
        let me = self.api.get_me().await?;
        let wh = self.api.get_webhook_info().await?;
        tracing::info!(
            bot = %me.username,
            pending = wh.pending_update_count,
            "health probe OK (T8)"
        );
        // If a webhook mysteriously appeared (T3 invariant), delete it.
        if !wh.url.is_empty() {
            tracing::warn!("webhook appeared during poll -- deleting (T3)");
            self.api.delete_webhook().await?;
        }
        Ok(())
    }

}

/// Render the response text for an inbound update, if any.
///
/// Pure function over `&BusDb` (read-only commands). Returns `None` when the
/// update is not a `/command` we can respond to — the row is still moved to
/// `dispatched` (it was handled, just with no outbox write).
///
/// callback_query dispatch is reserved for Phase 2.5 (HMAC callback tokens).
fn render_response(update: &Update, db: &BusDb) -> Option<(i64, String)> {
    let msg = update.message.as_ref()?;
    let text = msg.text.as_ref()?;
    if !text.starts_with('/') {
        return None;
    }
    Some((msg.chat.id, crate::commands::handle(text.trim(), db)))
}

// --- T5 backoff ladder -------------------------------------------------------

/// Returns backoff seconds for the n-th consecutive error: [1,2,4,8,16,32].
pub fn backoff_secs(attempt: u32) -> u64 {
    const LADDER: [u64; 6] = [1, 2, 4, 8, 16, 32];
    LADDER[(attempt.saturating_sub(1) as usize).min(5)]
}
