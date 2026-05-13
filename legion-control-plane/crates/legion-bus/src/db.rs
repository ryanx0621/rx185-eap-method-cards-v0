use anyhow::Context;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

use crate::BusResult;

/// Opens a SQLite connection with WAL mode and correct timeout settings.
///
/// Spec §9: SQLite WAL primary store.
pub fn open_connection(path: &Path) -> BusResult<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("opening SQLite database")?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;
         PRAGMA wal_autocheckpoint = 1000;",
    )
    .context("setting PRAGMA options")?;

    Ok(conn)
}

/// Wraps a SQLite connection and runs migrations on first open.
pub struct BusDb {
    pub conn: Connection,
}

impl BusDb {
    const MIGRATION_001_INIT: &'static str =
        include_str!("../../../sql/migrations/001_init.sql");

    pub fn open(path: &Path) -> BusResult<Self> {
        let conn = open_connection(path)?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    /// Opens an in-memory database for testing.
    pub fn open_in_memory() -> BusResult<Self> {
        let conn = Connection::open_in_memory().context("opening in-memory SQLite")?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )
        .context("setting PRAGMA options")?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> BusResult<()> {
        // 001: baseline schema (idempotent via CREATE TABLE IF NOT EXISTS).
        self.conn
            .execute_batch(Self::MIGRATION_001_INIT)
            .context("running 001_init")?;

        // 002: telegram_updates dispatch state machine — must inspect
        // PRAGMA table_info before ALTER, because SQLite's ADD COLUMN
        // lacks IF NOT EXISTS support across all versions.
        self.migrate_002_telegram_dispatch_state()
            .context("running 002_telegram_dispatch_state")?;

        Ok(())
    }

    /// Idempotent ALTERs guarded by PRAGMA table_info introspection,
    /// plus the legacy `received` → `pending_dispatch` data migration.
    fn migrate_002_telegram_dispatch_state(&self) -> BusResult<()> {
        let columns = self.table_columns("telegram_updates")?;

        if !columns.iter().any(|c| c == "dispatched_at") {
            self.conn
                .execute("ALTER TABLE telegram_updates ADD COLUMN dispatched_at TEXT", [])
                .context("adding telegram_updates.dispatched_at")?;
        }
        if !columns.iter().any(|c| c == "dispatch_error") {
            self.conn
                .execute("ALTER TABLE telegram_updates ADD COLUMN dispatch_error TEXT", [])
                .context("adding telegram_updates.dispatch_error")?;
        }

        // Migrate any legacy rows. Idempotent: only touches `received`.
        self.conn
            .execute(
                "UPDATE telegram_updates SET status='pending_dispatch' WHERE status='received'",
                [],
            )
            .context("migrating received -> pending_dispatch")?;

        // Partial index covering active dispatch work.
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_telegram_updates_pending
                    ON telegram_updates(status, update_id)
                    WHERE status='pending_dispatch'",
                [],
            )
            .context("creating idx_telegram_updates_pending")?;

        Ok(())
    }

    /// Read the column names of `table_name` via `PRAGMA table_info`.
    /// Column 1 of table_info is the column name (column 0 is its ordinal).
    fn table_columns(&self, table_name: &str) -> BusResult<Vec<String>> {
        let sql = format!("PRAGMA table_info({table_name})");
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("preparing PRAGMA table_info")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("querying PRAGMA table_info")?;
        Ok(rows.flatten().collect())
    }

    /// Returns true if RED_STOP is active.
    pub fn is_red_stop(&self) -> BusResult<bool> {
        let red_stop: i64 = self
            .conn
            .query_row(
                "SELECT red_stop FROM bus_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .context("reading bus_state red_stop")?;
        Ok(red_stop != 0)
    }

    /// Activate RED_STOP — blocks all mutation paths. Only repair/diagnostics remain.
    pub fn set_red_stop(&self, reason: &str) -> BusResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "UPDATE bus_state SET red_stop = 1, red_stop_reason = ?1, red_stop_at = ?2
                 WHERE singleton = 1",
                rusqlite::params![reason, now],
            )
            .context("setting RED_STOP")?;
        tracing::error!(reason = reason, "RED_STOP activated");
        Ok(())
    }

    /// Clear RED_STOP after manual operator verification.
    pub fn clear_red_stop(&self) -> BusResult<()> {
        self.conn
            .execute(
                "UPDATE bus_state SET red_stop = 0, red_stop_reason = NULL, red_stop_at = NULL
                 WHERE singleton = 1",
                [],
            )
            .context("clearing RED_STOP")?;
        tracing::warn!("RED_STOP cleared by operator");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Construct an in-memory DB with the legacy (pre-Phase-2.2) telegram_updates
    /// schema, then run migrations and verify the new columns + data are in place.
    /// This proves the PRAGMA-table_info path works on real legacy DBs, not just
    /// on fresh 001_init schemas.
    #[test]
    fn legacy_telegram_updates_schema_migrates_to_phase_2_2() {
        let conn = Connection::open_in_memory().unwrap();

        // 1. Hand-craft the legacy schema (pre-Phase-2.2):
        //    no dispatched_at, no dispatch_error, default status='received'.
        conn.execute_batch(
            "CREATE TABLE telegram_updates (
                update_id   INTEGER PRIMARY KEY,
                raw_json    TEXT NOT NULL,
                received_at TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'received'
             );",
        )
        .unwrap();

        // 2. Seed a legacy row with status='received'.
        conn.execute(
            "INSERT INTO telegram_updates (update_id, raw_json, received_at, status)
             VALUES (1, '{}', '2026-01-01T00:00:00Z', 'received')",
            [],
        )
        .unwrap();

        // 3. Wrap in BusDb and run migrations.
        let db = BusDb { conn };
        db.migrate().expect("migrations on legacy schema");

        // 4. New columns must exist.
        let cols = db.table_columns("telegram_updates").unwrap();
        assert!(cols.iter().any(|c| c == "dispatched_at"), "dispatched_at added");
        assert!(cols.iter().any(|c| c == "dispatch_error"), "dispatch_error added");

        // 5. Legacy 'received' row must have been migrated to 'pending_dispatch'.
        let status: String = db
            .conn
            .query_row(
                "SELECT status FROM telegram_updates WHERE update_id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "pending_dispatch", "legacy row migrated to pending_dispatch");

        // 6. Migration must be idempotent: running again must not error.
        db.migrate().expect("re-running migrations is idempotent");
    }
}
