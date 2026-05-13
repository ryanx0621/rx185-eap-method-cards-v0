use anyhow::Context;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use uuid::Uuid;

use legion_types::event::EventType;
use legion_types::lease::{AuthorityLease, LeaseScope, LeaseStatus};

use crate::{BusDb, BusError, BusResult};
use crate::event_log::EventLog;

/// Telegram poller lease TTL (T1, spec §6).
pub const TELEGRAM_POLLER_LEASE_TTL_SECS: u64 = 15;
/// Telegram poller refresh interval.
pub const TELEGRAM_POLLER_REFRESH_SECS: u64 = 5;

pub struct LeaseManager<'a> {
    db: &'a BusDb,
}

impl<'a> LeaseManager<'a> {
    pub fn new(db: &'a BusDb) -> Self {
        Self { db }
    }

    /// Attempt to acquire a lease. Returns Ok(lease_id) on success.
    ///
    /// Fails if another holder already has an active, unexpired lease for this scope.
    /// Spec T1: only one active `TelegramPoller` lease is allowed at a time.
    pub fn acquire(&self, scope: LeaseScope, holder_id: &str, ttl_secs: u64) -> BusResult<Uuid> {
        let scope_str = serde_json::to_string(&scope)?;
        // Enum variants are JSON-encoded (e.g. `"active"` with quotes) in the TEXT column.
        let active_str = serde_json::to_string(&LeaseStatus::Active)?;
        let now = Utc::now();

        // Check for existing active lease for this scope (single-writer invariant).
        let existing: Option<(String, String)> = self
            .db
            .conn
            .query_row(
                "SELECT lease_id, holder_id FROM authority_leases
                  WHERE scope = ?1 AND status = ?2 AND expires_at > ?3
                  LIMIT 1",
                params![scope_str, active_str, now.to_rfc3339()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("checking existing lease")?;

        if let Some((_, existing_holder)) = existing {
            if existing_holder != holder_id {
                return Err(BusError::LeaseNotHeld {
                    scope: scope_str,
                    holder: holder_id.to_owned(),
                });
            }
            // Same holder already has a lease — allow re-acquire (idempotent for same holder).
        }

        let lease = AuthorityLease::new(scope, holder_id, ttl_secs);
        let lease_id_str = lease.lease_id.to_string();
        let status_str = serde_json::to_string(&LeaseStatus::Active)?;

        self.db.conn.execute(
            "INSERT INTO authority_leases
                (lease_id, scope, holder_id, status, ttl_secs, acquired_at, expires_at, last_renewed_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?7)",
            params![
                lease_id_str,
                serde_json::to_string(&lease.scope)?,
                lease.holder_id,
                status_str,
                lease.ttl_secs as i64,
                lease.acquired_at.to_rfc3339(),
                lease.expires_at.to_rfc3339(),
            ],
        ).context("inserting lease")?;

        let log = EventLog::new(self.db);
        log.append(
            EventType::LeaseAcquired,
            Some(holder_id),
            None,
            None,
            None,
            serde_json::json!({
                "lease_id": lease_id_str,
                "scope": lease.scope,
                "ttl_secs": ttl_secs,
            }),
        )?;

        tracing::info!(
            lease_id = %lease_id_str, holder = holder_id,
            "lease acquired"
        );
        Ok(lease.lease_id)
    }

    /// Renew a lease, extending its expiry by another TTL.
    pub fn renew(&self, lease_id: Uuid, holder_id: &str) -> BusResult<()> {
        let lease_id_str = lease_id.to_string();
        let now = Utc::now();
        let ttl: Option<i64> = self
            .db
            .conn
            .query_row(
                "SELECT ttl_secs FROM authority_leases
                  WHERE lease_id = ?1 AND holder_id = ?2 AND status = 'active'",
                params![lease_id_str, holder_id],
                |row| row.get(0),
            )
            .optional()
            .context("querying lease for renewal")?;

        let ttl = match ttl {
            Some(t) => t,
            None => return Err(BusError::LeaseNotHeld {
                scope: "unknown".into(),
                holder: holder_id.to_owned(),
            }),
        };

        let new_expiry = (now + chrono::Duration::seconds(ttl)).to_rfc3339();
        self.db.conn.execute(
            "UPDATE authority_leases SET expires_at = ?1, last_renewed_at = ?2
             WHERE lease_id = ?3",
            params![new_expiry, now.to_rfc3339(), lease_id_str],
        )?;

        Ok(())
    }

    /// Expire/revoke a lease.
    pub fn revoke(&self, lease_id: Uuid) -> BusResult<()> {
        let lease_id_str = lease_id.to_string();
        let revoked_str = serde_json::to_string(&LeaseStatus::Revoked)?;
        self.db.conn.execute(
            "UPDATE authority_leases SET status = ?1 WHERE lease_id = ?2",
            params![revoked_str, lease_id_str],
        )?;

        let log = EventLog::new(self.db);
        log.append(
            EventType::LeaseRevoked,
            None,
            None,
            None,
            None,
            serde_json::json!({ "lease_id": lease_id_str }),
        )?;

        Ok(())
    }

    /// Check that a specific lease is still valid (active + not expired).
    pub fn assert_valid(&self, lease_id: Uuid, holder_id: &str) -> BusResult<()> {
        let lease_id_str = lease_id.to_string();
        let active_str = serde_json::to_string(&LeaseStatus::Active)?;
        let now = Utc::now().to_rfc3339();
        let valid: Option<i64> = self
            .db
            .conn
            .query_row(
                "SELECT 1 FROM authority_leases
                  WHERE lease_id = ?1 AND holder_id = ?2
                    AND status = ?3 AND expires_at > ?4",
                params![lease_id_str, holder_id, active_str, now],
                |row| row.get(0),
            )
            .optional()
            .context("asserting lease validity")?;

        if valid.is_none() {
            return Err(BusError::LeaseNotHeld {
                scope: "n/a".into(),
                holder: holder_id.to_owned(),
            });
        }
        Ok(())
    }
}
