use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Named lease scopes used by the Bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseScope {
    /// Single-writer Telegram poller (T1). TTL 15s, refresh every 5s.
    TelegramPoller,
    /// A provider session (PTY lifetime). One per agent session.
    ProviderSession,
    /// Task authority — held by the assignee for the task duration.
    TaskAuthority,
    /// Outbox worker lease — per-chat or global.
    OutboxWorker,
    /// Custom / extension scope.
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseStatus {
    Active,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityLease {
    pub lease_id: Uuid,
    pub scope: LeaseScope,
    pub holder_id: String,
    pub status: LeaseStatus,
    pub ttl_secs: u64,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_renewed_at: DateTime<Utc>,
}

impl AuthorityLease {
    pub fn new(scope: LeaseScope, holder_id: impl Into<String>, ttl_secs: u64) -> Self {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(ttl_secs as i64);
        Self {
            lease_id: Uuid::new_v4(),
            scope,
            holder_id: holder_id.into(),
            status: LeaseStatus::Active,
            ttl_secs,
            acquired_at: now,
            expires_at,
            last_renewed_at: now,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.status == LeaseStatus::Active && Utc::now() < self.expires_at
    }
}
