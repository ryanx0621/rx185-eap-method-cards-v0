use anyhow::Context;
use chrono::Utc;
use rusqlite::params;

use legion_types::command::{CommandSource, CommandStatus, RemoteCommand, RiskClass};
use legion_types::event::EventType;

use crate::{BusDb, BusError, BusResult};
use crate::event_log::EventLog;

pub struct CommandQueue<'a> {
    db: &'a BusDb,
}

impl<'a> CommandQueue<'a> {
    pub fn new(db: &'a BusDb) -> Self {
        Self { db }
    }

    /// Enqueue a command with exactly-once semantics.
    ///
    /// Spec §2: "command_id has a unique index. Duplicate execution is impossible."
    /// Spec §2: "every mutation appends an event before execution."
    ///
    /// Returns:
    ///   - `Ok(command_id)` if newly inserted.
    ///   - `Err(BusError::CommandDuplicate)` if already exists.
    ///   - `Err(BusError::RedStop)` if RED_STOP is active.
    pub fn enqueue(
        &self,
        source: CommandSource,
        source_event_id: impl Into<String>,
        actor_id: impl Into<String>,
        intent: serde_json::Value,
        risk_class: RiskClass,
    ) -> BusResult<String> {
        // Mutations blocked during RED_STOP (spec §9).
        if self.db.is_red_stop()? {
            let reason = self.db.conn.query_row(
                "SELECT COALESCE(red_stop_reason, 'unknown') FROM bus_state WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            ).unwrap_or_default();
            return Err(BusError::RedStop { reason });
        }

        let cmd = RemoteCommand::new(source, source_event_id, actor_id, intent, risk_class);
        let now = Utc::now().to_rfc3339();

        let initial_status = if matches!(
            cmd.risk_class,
            RiskClass::Destructive | RiskClass::SystemHighRisk | RiskClass::ProcessLifecycle
        ) {
            CommandStatus::NeedsReview
        } else {
            CommandStatus::Queued
        };

        let intent_str = serde_json::to_string(&cmd.intent)?;
        let source_str = serde_json::to_string(&cmd.source)?;
        let risk_str = serde_json::to_string(&cmd.risk_class)?;
        let status_str = serde_json::to_string(&initial_status)?;

        // Append event BEFORE insert (spec §2: "every mutation appends an event before execution").
        let log = EventLog::new(self.db);
        let event_id = log.append(
            EventType::RemoteCommandReceived,
            Some(&cmd.actor_id),
            None,
            None,
            Some(&cmd.command_id),
            serde_json::json!({
                "source": &cmd.source,
                "source_event_id": &cmd.source_event_id,
                "risk_class": &cmd.risk_class,
                "status": &initial_status,
            }),
        )?;

        // Insert with UNIQUE constraint on command_id.
        match self.db.conn.execute(
            "INSERT INTO remote_commands
                (command_id, source, source_event_id, actor_id, intent, risk_class, status,
                 audit_event_id, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?9)",
            params![
                cmd.command_id,
                source_str,
                cmd.source_event_id,
                cmd.actor_id,
                intent_str,
                risk_str,
                status_str,
                event_id,
                now,
            ],
        ) {
            Ok(_) => {
                // Also emit a Queued event for status tracking.
                log.append(
                    EventType::CommandQueued,
                    Some(&cmd.actor_id),
                    None,
                    None,
                    Some(&cmd.command_id),
                    serde_json::json!({ "status": initial_status }),
                )?;
                tracing::info!(command_id = %cmd.command_id, "command enqueued");
                Ok(cmd.command_id)
            }
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                log.append(
                    EventType::CommandDeduplicated,
                    Some(&cmd.actor_id),
                    None,
                    None,
                    Some(&cmd.command_id),
                    serde_json::json!({}),
                )?;
                tracing::debug!(command_id = %cmd.command_id, "command deduplicated");
                Err(BusError::CommandDuplicate {
                    command_id: cmd.command_id,
                })
            }
            Err(e) => Err(BusError::Sqlite(e)),
        }
    }

    /// Update command status (e.g. Queued → Executed after Bus dispatch).
    pub fn update_status(&self, command_id: &str, status: CommandStatus) -> BusResult<()> {
        let status_str = serde_json::to_string(&status)?;
        let now = Utc::now().to_rfc3339();
        let changed = self.db.conn.execute(
            "UPDATE remote_commands SET status = ?1, updated_at = ?2 WHERE command_id = ?3",
            params![status_str, now, command_id],
        )?;
        if changed == 0 {
            return Err(anyhow::anyhow!("command {command_id} not found").into());
        }
        Ok(())
    }

    /// Fetch a command by id.
    pub fn get(&self, command_id: &str) -> BusResult<Option<StoredCommand>> {
        Ok(self.db
            .conn
            .query_row(
                "SELECT command_id, source, actor_id, intent, risk_class, status, audit_event_id,
                         created_at
                  FROM remote_commands WHERE command_id = ?1",
                params![command_id],
                |row| {
                    Ok(StoredCommand {
                        command_id: row.get(0)?,
                        source: row.get(1)?,
                        actor_id: row.get(2)?,
                        intent: row.get(3)?,
                        risk_class: row.get(4)?,
                        status: row.get(5)?,
                        audit_event_id: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                },
            )
            .optional()
            .context("querying command")?)
    }
}

#[derive(Debug)]
pub struct StoredCommand {
    pub command_id: String,
    pub source: String,
    pub actor_id: String,
    pub intent: String,
    pub risk_class: String,
    pub status: String,
    pub audit_event_id: Option<String>,
    pub created_at: String,
}

use rusqlite::OptionalExtension;
