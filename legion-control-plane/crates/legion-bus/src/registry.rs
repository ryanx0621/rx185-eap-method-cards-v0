use anyhow::Context;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use legion_types::agent::{AgentProcess, AgentState};
use legion_types::event::EventType;

use crate::{BusDb, BusResult};
use crate::event_log::EventLog;

pub struct AgentRegistry<'a> {
    db: &'a BusDb,
}

impl<'a> AgentRegistry<'a> {
    pub fn new(db: &'a BusDb) -> Self {
        Self { db }
    }

    /// Register an agent for the first time or upsert if already present.
    pub fn register(&self, agent: &AgentProcess) -> BusResult<()> {
        let provider = serde_json::to_string(&agent.provider)?;
        let state = serde_json::to_string(&agent.state)?;
        let now = Utc::now().to_rfc3339();

        self.db.conn.execute(
            "INSERT INTO agent_registry
                (agent_id, provider, profile_id, state, pid, workspace, model_hint,
                 current_task_id, lease_id, registered_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10)
             ON CONFLICT(agent_id) DO UPDATE SET
                state = excluded.state,
                updated_at = excluded.updated_at",
            params![
                agent.agent_id,
                provider,
                agent.profile_id,
                state,
                agent.pid,
                agent.workspace,
                agent.model_hint,
                agent.current_task_id,
                agent.lease_id.map(|u| u.to_string()),
                now,
            ],
        )?;

        let log = EventLog::new(self.db);
        log.append(
            EventType::AgentRegistered,
            None,
            Some(&agent.agent_id),
            None,
            None,
            serde_json::json!({
                "provider": agent.provider,
                "profile_id": agent.profile_id,
            }),
        )?;

        tracing::info!(agent_id = %agent.agent_id, "agent registered");
        Ok(())
    }

    /// Transition an agent to a new state.
    pub fn set_state(&self, agent_id: &str, state: AgentState) -> BusResult<()> {
        let state_str = serde_json::to_string(&state)?;
        let now = Utc::now().to_rfc3339();
        self.db.conn.execute(
            "UPDATE agent_registry SET state = ?1, updated_at = ?2 WHERE agent_id = ?3",
            params![state_str, now, agent_id],
        )?;

        let log = EventLog::new(self.db);
        log.append(
            EventType::AgentStateChanged,
            None,
            Some(agent_id),
            None,
            None,
            serde_json::json!({ "new_state": state }),
        )?;

        Ok(())
    }

    /// Get current agent state.
    pub fn get(&self, agent_id: &str) -> BusResult<Option<StoredAgent>> {
        Ok(self.db
            .conn
            .query_row(
                "SELECT agent_id, provider, profile_id, state, pid, workspace,
                         model_hint, current_task_id, updated_at
                  FROM agent_registry WHERE agent_id = ?1",
                params![agent_id],
                |row| {
                    Ok(StoredAgent {
                        agent_id: row.get(0)?,
                        provider: row.get(1)?,
                        profile_id: row.get(2)?,
                        state: row.get(3)?,
                        pid: row.get(4)?,
                        workspace: row.get(5)?,
                        model_hint: row.get(6)?,
                        current_task_id: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                },
            )
            .optional()
            .context("querying agent")?)
    }

    /// List all agents, optionally filtered by state.
    pub fn list_by_state(&self, state: Option<&AgentState>) -> BusResult<Vec<StoredAgent>> {
        match state {
            None => {
                let mut stmt = self.db.conn.prepare(
                    "SELECT agent_id, provider, profile_id, state, pid, workspace,
                             model_hint, current_task_id, updated_at
                      FROM agent_registry ORDER BY agent_id",
                )?;
                let rows = stmt.query_map([], row_to_stored_agent)?
                    .collect::<Result<Vec<_>, _>>()
                    .context("listing agents")?;
                Ok(rows)
            }
            Some(s) => {
                let state_str = serde_json::to_string(s)?;
                let mut stmt = self.db.conn.prepare(
                    "SELECT agent_id, provider, profile_id, state, pid, workspace,
                             model_hint, current_task_id, updated_at
                      FROM agent_registry WHERE state = ?1 ORDER BY agent_id",
                )?;
                let rows = stmt.query_map(params![state_str], row_to_stored_agent)?
                    .collect::<Result<Vec<_>, _>>()
                    .context("listing agents by state")?;
                Ok(rows)
            }
        }
    }
}

fn row_to_stored_agent(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredAgent> {
    Ok(StoredAgent {
        agent_id: row.get(0)?,
        provider: row.get(1)?,
        profile_id: row.get(2)?,
        state: row.get(3)?,
        pid: row.get(4)?,
        workspace: row.get(5)?,
        model_hint: row.get(6)?,
        current_task_id: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

#[derive(Debug)]
pub struct StoredAgent {
    pub agent_id: String,
    pub provider: String,
    pub profile_id: String,
    pub state: String,
    pub pid: Option<u32>,
    pub workspace: Option<String>,
    pub model_hint: Option<String>,
    pub current_task_id: Option<String>,
    pub updated_at: String,
}
