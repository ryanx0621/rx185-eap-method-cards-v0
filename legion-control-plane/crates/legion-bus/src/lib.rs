pub mod commands;
pub mod db;
pub mod event_log;
pub mod heartbeat;
pub mod leases;
pub mod registry;

pub use db::BusDb;

/// Bus-wide error type.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Command duplicate: {command_id}")]
    CommandDuplicate { command_id: String },

    #[error("RED_STOP active: {reason}")]
    RedStop { reason: String },

    #[error("Event chain broken at event {event_id}: {detail}")]
    ChainBroken { event_id: String, detail: String },

    #[error("Lease not held: scope={scope}, holder={holder}")]
    LeaseNotHeld { scope: String, holder: String },

    #[error("Heartbeat rejected: clock drift {skew_ms}ms exceeds 60s limit")]
    TimeDriftRejected { skew_ms: i64 },

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type BusResult<T> = Result<T, BusError>;
