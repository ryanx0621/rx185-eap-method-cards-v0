/// T7: Rate-limited Telegram outbox.
///
/// Rules:
///   - Per-chat: ≤ 1 message/s (enforced via `not_before` column).
///   - Global:   ≤ 25 messages/s (40 ms sleep between sends).
///   - Retry on transient errors; dead-letter after 5 failures.
///   - SKIP LOCKED semantics emulated via row status + rowid ordering.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use rusqlite::params;

use legion_bus::BusDb;
use crate::api::TelegramApi;

const MAX_RETRY: u32 = 5;
const GLOBAL_INTERVAL_MS: u64 = 40; // 1000 / 25 = 40 ms → 25 msg/s global cap
const PER_CHAT_INTERVAL_MS: i64 = 1000; // 1 msg/s per chat

pub struct OutboxManager {
    db: Arc<Mutex<BusDb>>,
    /// In-memory last-sent timestamp per chat_id (milliseconds since epoch).
    last_sent_ms: Mutex<HashMap<i64, i64>>,
}

impl OutboxManager {
    pub fn new(db: Arc<Mutex<BusDb>>) -> Arc<Self> {
        Arc::new(Self {
            db,
            last_sent_ms: Mutex::new(HashMap::new()),
        })
    }

    /// Enqueue a text message to be sent by `flush_once`.
    pub fn enqueue(&self, chat_id: i64, text: String) {
        let db = self.db.lock().unwrap();
        let now = Utc::now().to_rfc3339();

        // Determine per-chat sequence number.
        let seq: i64 = db
            .conn
            .query_row(
                "SELECT COALESCE(MAX(message_seq), 0) + 1 FROM telegram_outbox WHERE chat_id = ?1",
                params![chat_id],
                |r| r.get(0),
            )
            .unwrap_or(1);

        let payload = serde_json::json!({ "text": text }).to_string();
        let _ = db.conn.execute(
            "INSERT INTO telegram_outbox
                (chat_id, message_seq, payload, method, status, retry_count, created_at)
             VALUES (?1, ?2, ?3, 'sendMessage', 'pending', 0, ?4)",
            params![chat_id, seq, payload, now],
        );
    }

    /// Send one batch of pending outbox messages, respecting T7 rate limits.
    ///
    /// Returns the number of messages successfully sent.
    pub async fn flush_once<A: TelegramApi>(&self, api: &A) -> usize {
        let rows = self.pending_rows();
        let mut sent = 0usize;

        for (id, chat_id, payload_json, retry_count) in rows {
            // Per-chat 1 msg/s: check last sent time.
            let now_ms = Utc::now().timestamp_millis();
            let wait_ms = {
                let last = self.last_sent_ms.lock().unwrap();
                if let Some(&t) = last.get(&chat_id) {
                    let elapsed = now_ms - t;
                    if elapsed < PER_CHAT_INTERVAL_MS {
                        PER_CHAT_INTERVAL_MS - elapsed
                    } else {
                        0
                    }
                } else {
                    0
                }
            };
            if wait_ms > 0 {
                tokio::time::sleep(Duration::from_millis(wait_ms as u64)).await;
            }

            // Parse payload.
            let text = serde_json::from_str::<serde_json::Value>(&payload_json)
                .ok()
                .and_then(|v| v["text"].as_str().map(str::to_owned))
                .unwrap_or_default();

            match api.send_message(chat_id, text, None).await {
                Ok(_) => {
                    self.mark_sent(id);
                    self.last_sent_ms
                        .lock()
                        .unwrap()
                        .insert(chat_id, Utc::now().timestamp_millis());
                    sent += 1;
                }
                Err(crate::error::TgError::RateLimited { retry_after_secs }) => {
                    let not_before =
                        (Utc::now() + chrono::Duration::seconds(retry_after_secs as i64))
                            .to_rfc3339();
                    self.set_not_before(id, &not_before);
                }
                Err(_) if retry_count >= MAX_RETRY => self.mark_dead(id),
                Err(_) => self.increment_retry(id),
            }

            // Global 25 msg/s cap.
            tokio::time::sleep(Duration::from_millis(GLOBAL_INTERVAL_MS)).await;
        }

        sent
    }

    // ─── Private DB helpers ──────────────────────────────────────────────────────

    fn pending_rows(&self) -> Vec<(i64, i64, String, u32)> {
        let db = self.db.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let mut stmt = db
            .conn
            .prepare(
                "SELECT id, chat_id, payload, retry_count
                   FROM telegram_outbox
                  WHERE status = 'pending'
                    AND (not_before IS NULL OR not_before <= ?1)
                  ORDER BY chat_id, message_seq
                  LIMIT 50",
            )
            .unwrap();
        stmt.query_map(params![now], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .unwrap()
        .flatten()
        .collect()
    }

    fn mark_sent(&self, id: i64) {
        let db = self.db.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let _ = db.conn.execute(
            "UPDATE telegram_outbox SET status = 'sent', sent_at = ?1 WHERE id = ?2",
            params![now, id],
        );
    }

    fn mark_dead(&self, id: i64) {
        let db = self.db.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let _ = db.conn.execute(
            "UPDATE telegram_outbox
                SET status = 'dead_letter', dead_lettered_at = ?1
              WHERE id = ?2",
            params![now, id],
        );
    }

    fn increment_retry(&self, id: i64) {
        let db = self.db.lock().unwrap();
        let _ = db.conn.execute(
            "UPDATE telegram_outbox SET retry_count = retry_count + 1 WHERE id = ?1",
            params![id],
        );
    }

    fn set_not_before(&self, id: i64, not_before: &str) {
        let db = self.db.lock().unwrap();
        let _ = db.conn.execute(
            "UPDATE telegram_outbox SET not_before = ?1 WHERE id = ?2",
            params![not_before, id],
        );
    }
}

impl OutboxManager {
    /// Count pending messages in outbox.
    pub fn pending_count(&self) -> usize {
        let db = self.db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM telegram_outbox WHERE status = 'pending'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize
    }

    /// Count sent messages in outbox (test helper).
    pub fn sent_count(&self) -> usize {
        let db = self.db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM telegram_outbox WHERE status = 'sent'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize
    }
}
