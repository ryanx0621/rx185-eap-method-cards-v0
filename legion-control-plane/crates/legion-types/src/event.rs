use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// All event types emitted by the Bus. Used as the discriminant in EventRecord.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum EventType {
    // Agent lifecycle
    AgentRegistered,
    AgentHeartbeat,
    AgentStateChanged,
    AgentOffline,
    TimeDriftRejected,
    // Task
    TaskCreated,
    TaskAssigned,
    TaskHeartbeatMissed,
    TaskEscalated,
    TaskClosed,
    // Commands
    RemoteCommandReceived,
    CommandQueued,
    CommandDeduplicated,
    CommandRejected,
    CommandExecuted,
    CommandFailed,
    // Review / approval
    ReviewRequested,
    ReviewApproved,
    ReviewDenied,
    RedStopRaised,
    RedStopCleared,
    // Event chain
    EventChainBroken,
    EventChainVerified,
    // Artifacts
    ArtifactWritten,
    // Telegram
    TelegramUpdateReceived,
    TelegramUpdateDeduplicated,
    TelegramCommandParsed,
    TelegramCommandRejected,
    TelegramCommandQueued,
    TelegramAckSent,
    TelegramOutboxQueued,
    TelegramOutboxSent,
    TelegramOutboxFailed,
    TelegramDegraded,
    TelegramWebhookRejected,
    // Leases
    LeaseAcquired,
    LeaseRenewed,
    LeaseExpired,
    LeaseRevoked,
    // Other
    Other(String),
}

/// A single immutable event in the append-only log.
///
/// Hash chain rule:
///   event_hash = sha256(canonical(event_without_event_hash field))
///   prev_event_hash = hash of immediately preceding committed event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub event_id: String,
    pub event_type: EventType,
    pub actor_id: Option<String>,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub command_id: Option<String>,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    /// Hash of the previous event in the chain. Empty string for genesis event.
    pub prev_event_hash: String,
    /// sha256(canonical(this event with prev_event_hash set, event_hash=""))
    pub event_hash: String,
}

impl EventRecord {
    /// Compute the event_hash from a partially constructed record
    /// (event_hash field must be "" or absent before calling this).
    pub fn compute_hash(
        event_id: &str,
        event_type: &EventType,
        actor_id: Option<&str>,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        command_id: Option<&str>,
        payload: &serde_json::Value,
        created_at: &DateTime<Utc>,
        prev_event_hash: &str,
    ) -> String {
        // Canonical JSON with sorted keys for determinism
        let canonical = serde_json::json!({
            "event_id": event_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "agent_id": agent_id,
            "task_id": task_id,
            "command_id": command_id,
            "payload": payload,
            "created_at": created_at.to_rfc3339(),
            "prev_event_hash": prev_event_hash,
            "event_hash": ""
        });
        let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
        let hash = Sha256::digest(&bytes);
        hex::encode(hash)
    }

    pub fn new(
        event_id: String,
        event_type: EventType,
        actor_id: Option<String>,
        agent_id: Option<String>,
        task_id: Option<String>,
        command_id: Option<String>,
        payload: serde_json::Value,
        prev_event_hash: String,
    ) -> Self {
        let created_at = Utc::now();
        let event_hash = Self::compute_hash(
            &event_id,
            &event_type,
            actor_id.as_deref(),
            agent_id.as_deref(),
            task_id.as_deref(),
            command_id.as_deref(),
            &payload,
            &created_at,
            &prev_event_hash,
        );
        Self {
            event_id,
            event_type,
            actor_id,
            agent_id,
            task_id,
            command_id,
            payload,
            created_at,
            prev_event_hash,
            event_hash,
        }
    }

    /// Verify that this event's hash matches its content.
    pub fn verify(&self) -> bool {
        let expected = Self::compute_hash(
            &self.event_id,
            &self.event_type,
            self.actor_id.as_deref(),
            self.agent_id.as_deref(),
            self.task_id.as_deref(),
            self.command_id.as_deref(),
            &self.payload,
            &self.created_at,
            &self.prev_event_hash,
        );
        expected == self.event_hash
    }
}
