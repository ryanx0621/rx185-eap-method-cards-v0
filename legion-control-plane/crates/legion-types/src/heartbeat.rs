use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Spec §5: heartbeat fields include wall_ts, mono_ns, agent_clock_skew_ms.
/// Absolute drift > 60s → TimeDriftRejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatEvent {
    pub heartbeat_id: String,
    pub agent_id: String,
    pub task_id: Option<String>,
    /// Wall clock as reported by the agent (UTC).
    pub wall_ts: DateTime<Utc>,
    /// Monotonic nanosecond counter from the agent process start.
    pub mono_ns: u64,
    /// Pre-computed skew: agent wall_ts minus Bus receive time, in milliseconds.
    pub agent_clock_skew_ms: i64,
    pub progress_note: Option<String>,
    pub blocker: Option<String>,
    pub next_action: Option<String>,
    pub received_at: DateTime<Utc>,
}

/// Heartbeat policy constants (spec §5).
pub struct HeartbeatPolicy;

impl HeartbeatPolicy {
    pub const AGENT_INTERVAL_SECS: u64 = 30;
    pub const AGENT_STALE_SECS: u64 = 90;
    pub const AGENT_DEAD_SECS: u64 = 300;
    pub const TASK_INTERVAL_SECS: u64 = 180;
    pub const TASK_STALE_SECS: u64 = 540;
    /// Reject heartbeats where |agent_clock_skew_ms| > 60_000 ms.
    pub const MAX_DRIFT_MS: i64 = 60_000;

    pub fn is_drift_rejected(skew_ms: i64) -> bool {
        skew_ms.abs() > Self::MAX_DRIFT_MS
    }
}
