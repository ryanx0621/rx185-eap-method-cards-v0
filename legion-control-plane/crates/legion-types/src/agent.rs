use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Offline,
    Starting,
    Idle,
    Working,
    Blocked,
    Review,
    Stopping,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Claude,
    Codex,
    Kimi,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProcess {
    pub agent_id: String,
    pub provider: ProviderKind,
    /// CLI profile name (e.g. "ryanx-main"). Credential stays in native CLI.
    pub profile_id: String,
    pub state: AgentState,
    pub pid: Option<u32>,
    pub workspace: Option<String>,
    pub model_hint: Option<String>,
    pub current_task_id: Option<String>,
    pub lease_id: Option<Uuid>,
    pub registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AgentProcess {
    pub fn new(agent_id: impl Into<String>, provider: ProviderKind, profile_id: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            agent_id: agent_id.into(),
            provider,
            profile_id: profile_id.into(),
            state: AgentState::Offline,
            pid: None,
            workspace: None,
            model_hint: None,
            current_task_id: None,
            lease_id: None,
            registered_at: now,
            updated_at: now,
        }
    }
}
