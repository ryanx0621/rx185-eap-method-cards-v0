use anyhow::Context;
use chrono::Utc;
use rusqlite::params;
use uuid::Uuid;

use legion_types::event::{EventRecord, EventType};

use crate::{BusDb, BusError, BusResult};

/// Number of events verified on startup (spec §9).
const STARTUP_VERIFY_COUNT: u64 = 1000;

pub struct EventLog<'a> {
    db: &'a BusDb,
}

impl<'a> EventLog<'a> {
    pub fn new(db: &'a BusDb) -> Self {
        Self { db }
    }

    /// Fetch the hash of the most recently committed event. Empty string for genesis.
    fn last_event_hash(&self) -> BusResult<String> {
        let hash: Option<String> = self
            .db
            .conn
            .query_row(
                "SELECT event_hash FROM event_log ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .context("querying last event hash")?;
        Ok(hash.unwrap_or_default())
    }

    /// Append an event to the log, computing its hash against the previous event.
    ///
    /// Spec §9: "every mutation appends an event before execution."
    /// The event_hash is computed here; callers do not supply it.
    pub fn append(
        &self,
        event_type: EventType,
        actor_id: Option<&str>,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        command_id: Option<&str>,
        payload: serde_json::Value,
    ) -> BusResult<String> {
        let prev_hash = self.last_event_hash()?;
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let record = EventRecord::new(
            event_id.clone(),
            event_type,
            actor_id.map(str::to_owned),
            agent_id.map(str::to_owned),
            task_id.map(str::to_owned),
            command_id.map(str::to_owned),
            payload,
            prev_hash,
        );

        self.db
            .conn
            .execute(
                "INSERT INTO event_log
                    (event_id, event_type, actor_id, agent_id, task_id, command_id,
                     payload, created_at, prev_event_hash, event_hash)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    record.event_id,
                    serde_json::to_string(&record.event_type)?,
                    record.actor_id,
                    record.agent_id,
                    record.task_id,
                    record.command_id,
                    serde_json::to_string(&record.payload)?,
                    record.created_at.to_rfc3339(),
                    record.prev_event_hash,
                    record.event_hash,
                ],
            )
            .context("inserting event")?;

        tracing::debug!(event_id = %record.event_id, "event appended");
        Ok(event_id)
    }

    /// Verify the hash chain of the last N events.
    ///
    /// Spec §9: startup verifies last 1000; chain break → RED_STOP.
    pub fn verify_chain(&self, count: u64) -> BusResult<u64> {
        struct Row {
            event_id: String,
            event_type: String,
            actor_id: Option<String>,
            agent_id: Option<String>,
            task_id: Option<String>,
            command_id: Option<String>,
            payload: String,
            created_at: String,
            prev_event_hash: String,
            event_hash: String,
        }

        let mut stmt = self.db.conn.prepare(
            "SELECT event_id, event_type, actor_id, agent_id, task_id, command_id,
                    payload, created_at, prev_event_hash, event_hash
             FROM event_log
             ORDER BY rowid DESC
             LIMIT ?1",
        )?;

        let rows: Vec<Row> = stmt
            .query_map(params![count], |row| {
                Ok(Row {
                    event_id: row.get(0)?,
                    event_type: row.get(1)?,
                    actor_id: row.get(2)?,
                    agent_id: row.get(3)?,
                    task_id: row.get(4)?,
                    command_id: row.get(5)?,
                    payload: row.get(6)?,
                    created_at: row.get(7)?,
                    prev_event_hash: row.get(8)?,
                    event_hash: row.get(9)?,
                })
            })?
            .collect::<Result<_, _>>()
            .context("loading events for chain verify")?;

        let mut verified = 0u64;
        let mut expected_prev_hash: Option<String> = None;

        // Rows come in DESC order; iterate in ascending order to verify chain.
        for row in rows.iter().rev() {
            let event_type: EventType =
                serde_json::from_str(&row.event_type).unwrap_or(EventType::Other(row.event_type.clone()));
            let payload: serde_json::Value =
                serde_json::from_str(&row.payload).unwrap_or(serde_json::Value::Null);
            let created_at: chrono::DateTime<Utc> =
                row.created_at.parse().context("parsing event created_at")?;

            let computed = EventRecord::compute_hash(
                &row.event_id,
                &event_type,
                row.actor_id.as_deref(),
                row.agent_id.as_deref(),
                row.task_id.as_deref(),
                row.command_id.as_deref(),
                &payload,
                &created_at,
                &row.prev_event_hash,
            );

            if computed != row.event_hash {
                return Err(BusError::ChainBroken {
                    event_id: row.event_id.clone(),
                    detail: format!(
                        "event_hash mismatch: stored={}, computed={}",
                        row.event_hash, computed
                    ),
                });
            }

            // Verify chain linkage (skip for first event processed).
            if let Some(ref prev) = expected_prev_hash {
                if &row.prev_event_hash != prev {
                    return Err(BusError::ChainBroken {
                        event_id: row.event_id.clone(),
                        detail: format!(
                            "prev_event_hash mismatch: expected={prev}, got={}",
                            row.prev_event_hash
                        ),
                    });
                }
            }

            expected_prev_hash = Some(row.event_hash.clone());
            verified += 1;
        }

        Ok(verified)
    }

    /// Run startup verification. Activates RED_STOP if chain is broken (spec §9).
    pub fn startup_verify(&self) -> BusResult<()> {
        let now = Utc::now().to_rfc3339();
        match self.verify_chain(STARTUP_VERIFY_COUNT) {
            Ok(n) => {
                self.db.conn.execute(
                    "UPDATE bus_state SET last_chain_verify_at = ?1, chain_verified_events = ?2
                     WHERE singleton = 1",
                    params![now, n as i64],
                )?;
                tracing::info!(verified = n, "event chain OK");
                Ok(())
            }
            Err(BusError::ChainBroken { ref event_id, ref detail }) => {
                let reason = format!("chain broken at {event_id}: {detail}");
                self.db.set_red_stop(&reason)?;
                Err(BusError::ChainBroken {
                    event_id: event_id.clone(),
                    detail: detail.clone(),
                })
            }
            Err(e) => Err(e),
        }
    }
}

// Bring in the .optional() extension from rusqlite.
use rusqlite::OptionalExtension;
