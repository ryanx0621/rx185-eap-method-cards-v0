use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Risk classification determines whether a command executes directly or enters review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskClass {
    ReadOnly,
    LowMutation,
    SharedMemoryWrite,
    ProcessLifecycle,
    Destructive,
    SecretSensitive,
    SystemHighRisk,
}

/// Source of a remote command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandSource {
    Telegram,
    Pwa,
    Mcp,
    Internal,
}

/// Execution status of a RemoteCommand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Queued,
    NeedsReview,
    Rejected,
    Executed,
    Failed,
}

/// A remote command with exactly-once semantics guaranteed by `command_id`.
///
/// `command_id` is deterministic:
///   SHA256("legion.v1|{source}|{source_event_id}|{actor_id}|{canonical_intent_json}")[..24]
/// This means retries / replays always produce the same ID → DB unique index rejects duplicates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteCommand {
    /// Derived deterministically — see [`RemoteCommand::derive_id`].
    pub command_id: String,
    pub source: CommandSource,
    /// Upstream event ID (Telegram update_id, PWA request UUID, etc.)
    pub source_event_id: String,
    pub actor_id: String,
    pub intent: serde_json::Value,
    pub risk_class: RiskClass,
    pub status: CommandStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub audit_event_id: Option<String>,
    pub review_id: Option<String>,
}

impl RemoteCommand {
    /// Derive a stable, deterministic command_id from its identity components.
    /// Collision resistance: SHA256 prefix of 24 hex chars = 96 bits.
    pub fn derive_id(
        source: &CommandSource,
        source_event_id: &str,
        actor_id: &str,
        canonical_intent: &str,
    ) -> String {
        let source_str = serde_json::to_string(source).unwrap_or_default();
        let input = format!("legion.v1|{source_str}|{source_event_id}|{actor_id}|{canonical_intent}");
        let hash = Sha256::digest(input.as_bytes());
        hex::encode(&hash[..12]) // 24 hex chars
    }

    pub fn new(
        source: CommandSource,
        source_event_id: impl Into<String>,
        actor_id: impl Into<String>,
        intent: serde_json::Value,
        risk_class: RiskClass,
    ) -> Self {
        let source_event_id = source_event_id.into();
        let actor_id = actor_id.into();
        let canonical_intent = serde_json::to_string(&intent).unwrap_or_default();
        let command_id = Self::derive_id(&source, &source_event_id, &actor_id, &canonical_intent);
        let now = Utc::now();
        Self {
            command_id,
            source,
            source_event_id,
            actor_id,
            intent,
            risk_class,
            status: CommandStatus::Queued,
            created_at: now,
            updated_at: now,
            audit_event_id: None,
            review_id: None,
        }
    }
}
