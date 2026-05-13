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
    const MIGRATION_SQL: &'static str = include_str!("../../../sql/migrations/001_init.sql");

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
        self.conn
            .execute_batch(Self::MIGRATION_SQL)
            .context("running initial migration")?;
        Ok(())
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
