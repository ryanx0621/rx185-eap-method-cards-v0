//! Legion Telegram Gateway — Phase 2 (read-only, T1–T8 hardened).
//!
//! Crate layout:
//!   api      — TelegramApi trait + ReqwestClient (feature = "http")
//!   types    — Telegram Bot API receive-path types
//!   error    — TgError / TgResult
//!   offset   — T2 offset persistence + T6 update_id idempotency
//!   poller   — T1–T8 long-poll loop
//!   outbox   — T7 rate-limited send queue
//!   commands — /status /agents /tasks read-only dispatchers

pub mod api;
pub mod commands;
pub mod error;
pub mod offset;
pub mod outbox;
pub mod poller;
pub mod types;

pub use api::TelegramApi;
pub use error::{TgError, TgResult};
pub use outbox::OutboxManager;
pub use poller::{Poller, PollerConfig};

#[cfg(feature = "http")]
pub use api::ReqwestClient;
