/// Errors that can originate from the Telegram gateway layer.
#[derive(Debug, thiserror::Error)]
pub enum TgError {
    #[error("Telegram HTTP {status}: {body}")]
    Http { status: u16, body: String },

    /// 429 — T5: caller must honor `retry_after_secs` before next attempt.
    #[error("Telegram rate-limited, retry_after={retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    /// 401 — T5: never retry; fail hard.
    #[error("Telegram Unauthorized (401): bad bot token")]
    Unauthorized,

    /// 404 — T5: never retry; fail hard.
    #[error("Telegram Not Found (404): bot not found")]
    NotFound,

    /// 409 — T3: webhook/polling conflict; run() must call deleteWebhook and retry.
    #[error("Telegram Conflict (409): webhook and polling are active simultaneously")]
    Conflict,

    #[error("Network error: {0}")]
    Network(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Bus error: {0}")]
    Bus(#[from] legion_bus::BusError),

    #[error("Lease lost — another poller took over")]
    LeaseLost,

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub type TgResult<T> = Result<T, TgError>;
