use anyhow::Context;
use chrono::Utc;
use rusqlite::params;
use uuid::Uuid;

use legion_types::event::EventType;
use legion_types::heartbeat::HeartbeatPolicy;

use crate::{BusDb, BusError, BusResult};
use crate::event_log::EventLog;

pub struct HeartbeatTracker<'a> {
    db: &'a BusDb,
}

impl<'a> HeartbeatTracker<'a> {
    pub fn new(db: &'a BusDb) -> Self {
        Self { db }
    }

    /// Record a heartbeat from an agent.
    ///
    /// Spec §5: rejects heartbeats where |agent_clock_skew_ms| > 60_000.
    pub fn record(&self, agent_id: &str, wall_ts: chrono::DateTime<Utc>, mono_ns: u64,
                  task_id: Option<&str>, progress_note: Option<&str>,
                  blocker: Option<&str>, next_action: Option<&str>) -> BusResult<String> {
        let received_at = Utc::now();
        let skew_ms = wall_ts.signed_duration_since(received_at).num_milliseconds();

        if HeartbeatPolicy::is_drift_rejected(skew_ms) {
            let log = EventLog::new(self.db);
            log.append(
                EventType::TimeDriftRejected,
                None,
                Some(agent_id),
                task_id,
                None,
                serde_json::json!({ "agent_clock_skew_ms": skew_ms }),
            )?;
            return Err(BusError::TimeDriftRejected { skew_ms });
        }

        let heartbeat_id = format!("hb_{}", Uuid::new_v4().simple());
        self.db.conn.execute(
            "INSERT INTO heartbeats
                (heartbeat_id, agent_id, task_id, wall_ts, mono_ns, agent_clock_skew_ms,
                 progress_note, blocker, next_action, received_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                heartbeat_id,
                agent_id,
                task_id,
                wall_ts.to_rfc3339(),
                mono_ns as i64,
                skew_ms,
                progress_note,
                blocker,
                next_action,
                received_at.to_rfc3339(),
            ],
        ).context("inserting heartbeat")?;

        // Update agent registry timestamp.
        self.db.conn.execute(
            "UPDATE agent_registry SET updated_at = ?1 WHERE agent_id = ?2",
            params![received_at.to_rfc3339(), agent_id],
        )?;

        // Update task heartbeat timestamp if applicable.
        if let Some(tid) = task_id {
            self.db.conn.execute(
                "UPDATE task_orders SET last_heartbeat_at = ?1 WHERE task_id = ?2",
                params![received_at.to_rfc3339(), tid],
            )?;
        }

        let log = EventLog::new(self.db);
        log.append(
            EventType::AgentHeartbeat,
            None,
            Some(agent_id),
            task_id,
            None,
            serde_json::json!({
                "agent_clock_skew_ms": skew_ms,
                "blocker": blocker,
            }),
        )?;

        Ok(heartbeat_id)
    }

    /// Returns agents whose last heartbeat is older than the stale threshold (spec §5).
    pub fn stale_agents(&self) -> BusResult<Vec<String>> {
        let stale_cutoff = (Utc::now()
            - chrono::Duration::seconds(HeartbeatPolicy::AGENT_STALE_SECS as i64))
            .to_rfc3339();

        // States are JSON-encoded strings in the column (e.g. `"offline"` with quotes).
        let offline = serde_json::to_string(&legion_types::agent::AgentState::Offline)?;
        let dead = serde_json::to_string(&legion_types::agent::AgentState::Dead)?;
        let stopping = serde_json::to_string(&legion_types::agent::AgentState::Stopping)?;

        let mut stmt = self.db.conn.prepare(
            "SELECT agent_id FROM agent_registry
             WHERE state NOT IN (?1, ?2, ?3)
               AND updated_at < ?4",
        )?;
        let ids = stmt
            .query_map(params![offline, dead, stopping, stale_cutoff], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()
            .context("querying stale agents")?;
        Ok(ids)
    }
}
