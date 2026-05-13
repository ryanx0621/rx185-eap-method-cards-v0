/// T2: Persisted getUpdates offset — written to SQLite BEFORE acking the batch.
///
/// On restart the poller loads this value so no update is lost or double-delivered.

use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use legion_bus::BusDb;
use crate::error::TgResult;

pub struct OffsetStore<'a> {
    db: &'a BusDb,
    bot_id: &'a str,
}

impl<'a> OffsetStore<'a> {
    pub fn new(db: &'a BusDb, bot_id: &'a str) -> Self {
        Self { db, bot_id }
    }

    /// Returns the stored offset, or 0 if this bot has no history yet.
    pub fn load(&self) -> TgResult<i64> {
        let result: Option<i64> = self
            .db
            .conn
            .query_row(
                "SELECT last_update_id FROM telegram_offsets WHERE bot_id = ?1",
                params![self.bot_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(result.unwrap_or(0))
    }

    /// Persist `offset` durably before the batch is considered consumed (T2).
    pub fn save(&self, offset: i64) -> TgResult<()> {
        let now = Utc::now().to_rfc3339();
        self.db.conn.execute(
            "INSERT INTO telegram_offsets (bot_id, last_update_id, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(bot_id) DO UPDATE SET
                last_update_id = excluded.last_update_id,
                updated_at     = excluded.updated_at",
            params![self.bot_id, offset, now],
        )?;
        Ok(())
    }
}

// ─── Owned helper for use across await points ───────────────────────────────

use std::sync::{Arc, Mutex};

pub fn load_offset(db: &Arc<Mutex<BusDb>>, bot_id: &str) -> TgResult<i64> {
    let db = db.lock().unwrap();
    OffsetStore::new(&db, bot_id).load()
}

pub fn save_offset(db: &Arc<Mutex<BusDb>>, bot_id: &str, offset: i64) -> TgResult<()> {
    let db = db.lock().unwrap();
    OffsetStore::new(&db, bot_id).save(offset)
}

// ─── Idempotency store for update_id (T6) ─────────────────────────────────

/// Returns `true` if this update_id is NEW (not yet seen).
///
/// Inserts into `telegram_updates` with status='pending_dispatch' on first
/// sight; ignores UNIQUE conflict on duplicates (T6 idempotency).
///
/// After return, the row is durable; dispatch is the dispatcher's
/// responsibility (see `Poller::dispatch_pending`). This decoupling
/// guarantees that a crash between record and dispatch cannot strand the
/// update — replay finds the row already `pending_dispatch` and dispatches it.
pub fn record_update(
    db: &Arc<Mutex<BusDb>>,
    update_id: i64,
    raw_json: &str,
) -> TgResult<bool> {
    let db = db.lock().unwrap();
    let now = Utc::now().to_rfc3339();
    let changed = db.conn.execute(
        "INSERT OR IGNORE INTO telegram_updates (update_id, raw_json, received_at, status)
         VALUES (?1, ?2, ?3, 'pending_dispatch')",
        params![update_id, raw_json, now],
    )?;
    Ok(changed > 0)
}
