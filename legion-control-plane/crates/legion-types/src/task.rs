use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Assigned,
    Accepted,
    Working,
    Blocked,
    Review,
    Done,
    Failed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPriority {
    Normal,
    Urgent,
    RedStop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptanceCriteria {
    pub id: String,
    pub description: String,
    pub satisfied: bool,
    pub evidence_path: Option<String>,
}

/// Spec §5: Task Bureaucracy Engine task record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOrder {
    pub task_id: String,
    pub title: String,
    pub description: Option<String>,
    pub creator: String,
    pub assignee: Option<String>,
    pub reviewers: Vec<String>,
    pub committee: Vec<String>,
    pub priority: TaskPriority,
    pub risk_level: RiskLevel,
    pub status: TaskStatus,
    pub acceptance_criteria: Vec<AcceptanceCriteria>,
    /// Seconds between required task heartbeats (default 180).
    pub heartbeat_interval_sec: u64,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskOrder {
    pub fn new(
        task_id: impl Into<String>,
        title: impl Into<String>,
        creator: impl Into<String>,
        risk_level: RiskLevel,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id: task_id.into(),
            title: title.into(),
            description: None,
            creator: creator.into(),
            assignee: None,
            reviewers: Vec::new(),
            committee: Vec::new(),
            priority: TaskPriority::Normal,
            risk_level,
            status: TaskStatus::Queued,
            acceptance_criteria: Vec::new(),
            heartbeat_interval_sec: 180,
            last_heartbeat_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// True if the task heartbeat is overdue (> 540s since last beat, spec §5).
    pub fn is_heartbeat_stale(&self) -> bool {
        match self.last_heartbeat_at {
            None => true,
            Some(last) => {
                let elapsed = Utc::now()
                    .signed_duration_since(last)
                    .num_seconds();
                elapsed > 540
            }
        }
    }
}
