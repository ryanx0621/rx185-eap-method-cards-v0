/// Telegram long-poll loop implementing spec T1-T8.
///
/// T1  Single-writer lease (TTL 15 s, refresh every 5 s).
/// T2  Offset written to DB before batch is considered consumed.
/// T3  On boot: getWebhookInfo -> deleteWebhook if a URL is set.
/// T4  getUpdates timeout=25, HTTP read timeout=35 s (set on ReqwestClient).
/// T5  Backoff ladder [1,2,4,8,16,32]s; skip 401/404; honour 429 retry_after.
/// T6  update_id uniqueness via INSERT OR IGNORE into telegram_updates.
/// T7  Outbox rate limiting (delegated to OutboxManager).
/// T8  getMe + getWebhookInfo health probe every 30 s.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

    /// Poll for one batch of updates.
    ///
    /// - Reads offset from DB (T2).
    /// - Calls getUpdates with T4 timeout.
    /// - Records ALL updates to `telegram_updates` durably (T2+T6).
    /// - Dispatches new updates.
    /// - Advances offset only AFTER durable ingest (T2).
    ///
    /// Crash safety: if we crash between record and save_offset, Telegram
    /// resends the same batch; INSERT OR IGNORE (T6) deduplicates on replay.
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

        // T2: durably ingest ALL updates into telegram_updates BEFORE advancing offset.
        // On crash here, Telegram resends the batch; T6 INSERT OR IGNORE deduplicates.
        let mut ingested: Vec<(&Update, bool)> = Vec::with_capacity(updates.len());
        for update in &updates {
            let raw = serde_json::to_string(update).unwrap_or_default();
            let is_new = record_update(&self.db, update.update_id, &raw)?;
            ingested.push((update, is_new));
        }

        let mut new_count = 0usize;
        for (update, is_new) in &ingested {
            if *is_new {
                new_count += 1;
                self.dispatch(update).await;
            } else {
                tracing::debug!(update_id = update.update_id, "duplicate update_id ignored (T6)");
            }
        }

        // T2: offset advances only AFTER durable ingest is complete.
        save_offset(&self.db, &self.config.bot_id, max_id + 1)?;

        Ok(new_count)
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

    /// Dispatch a single inbound update to the command handler.
    async fn dispatch(&self, update: &Update) {
        if let Some(msg) = &update.message {
            if let Some(text) = &msg.text {
                if text.starts_with('/') {
                    let chat_id = msg.chat.id;
                    let response = {
                        let db = self.db.lock().unwrap();
                        crate::commands::handle(text.trim(), &db)
                    };
                    self.outbox.enqueue(chat_id, response);
                }
            }
        }
        // callback_query dispatch reserved for Phase 2.5 (HMAC callback tokens).
    }
}

// --- T5 backoff ladder -------------------------------------------------------

/// Returns backoff seconds for the n-th consecutive error: [1,2,4,8,16,32].
pub fn backoff_secs(attempt: u32) -> u64 {
    const LADDER: [u64; 6] = [1, 2, 4, 8, 16, 32];
    LADDER[(attempt.saturating_sub(1) as usize).min(5)]
}
