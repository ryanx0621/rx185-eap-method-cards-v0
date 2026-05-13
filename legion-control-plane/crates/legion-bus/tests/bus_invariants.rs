/// Integration tests for Legion Bus Core invariants.
///
/// Covers spec §14 acceptance scenarios:
///   A – poller kill & restart (command_id dedup / no loss)
///   B – webhook stolen token (command rejected before execution)  [structural test]
///   C – two concurrent approvals (exactly-once via unique index)
///   D – agent clock drift > 60s (TimeDriftRejected)
///   E – event_log tamper (chain break → RED_STOP)

use chrono::Utc;
use legion_bus::{
    BusDb, BusError,
    commands::CommandQueue,
    event_log::EventLog,
    heartbeat::HeartbeatTracker,
    leases::{LeaseManager, TELEGRAM_POLLER_LEASE_TTL_SECS},
    registry::AgentRegistry,
};
use legion_types::{
    agent::{AgentProcess, ProviderKind},
    command::{CommandSource, RiskClass},
    event::EventType,
    lease::LeaseScope,
};

fn fresh_db() -> BusDb {
    BusDb::open_in_memory().expect("in-memory db")
}

// ─── Scenario A: poller restart — command_id dedup ────────────────────────────
#[test]
fn scenario_a_command_id_dedup_no_duplicate_execution() {
    let db = fresh_db();
    let queue = CommandQueue::new(&db);

    let intent = serde_json::json!({ "action": "list_agents" });

    // First enqueue succeeds.
    let id1 = queue
        .enqueue(
            CommandSource::Telegram,
            "update_42",
            "ryanx",
            intent.clone(),
            RiskClass::ReadOnly,
        )
        .expect("first enqueue");

    // Second enqueue with identical source_event_id + intent → same command_id → duplicate error.
    let err = queue
        .enqueue(
            CommandSource::Telegram,
            "update_42",
            "ryanx",
            intent.clone(),
            RiskClass::ReadOnly,
        )
        .expect_err("should be duplicate");

    assert!(
        matches!(err, BusError::CommandDuplicate { ref command_id } if command_id == &id1),
        "expected CommandDuplicate, got {err:?}"
    );

    // Only one command row exists.
    let count: i64 = db
        .conn
        .query_row("SELECT COUNT(*) FROM remote_commands", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "exactly one command row");
}

// ─── Scenario B: high-risk command enqueued as NeedsReview ────────────────────
// (Structural test: Telegram cannot directly execute destructive commands.)
#[test]
fn scenario_b_destructive_command_requires_review() {
    let db = fresh_db();
    let queue = CommandQueue::new(&db);

    let cmd_id = queue
        .enqueue(
            CommandSource::Telegram,
            "update_99",
            "ryanx",
            serde_json::json!({ "action": "delete_agent", "target": "SakuraX" }),
            RiskClass::Destructive,
        )
        .expect("enqueue destructive");

    let stored = queue.get(&cmd_id).unwrap().expect("stored");
    // Status must be NeedsReview, not Queued or Executed.
    assert!(
        stored.status.contains("needs_review"),
        "destructive command must start as needs_review, got: {}",
        stored.status
    );
}

// ─── Scenario C: two approvals, exactly-once via unique index ─────────────────
#[test]
fn scenario_c_exactly_once_approval() {
    let db = fresh_db();
    let queue = CommandQueue::new(&db);

    let intent = serde_json::json!({ "action": "approve_review", "review_id": "rev_001" });

    let _id1 = queue
        .enqueue(
            CommandSource::Pwa,
            "pwa_req_abc",
            "ryanx",
            intent.clone(),
            RiskClass::LowMutation,
        )
        .expect("first approval");

    // Second PWA client sends same logical action → same command_id → duplicate.
    let err = queue
        .enqueue(
            CommandSource::Pwa,
            "pwa_req_abc",
            "ryanx",
            intent.clone(),
            RiskClass::LowMutation,
        )
        .expect_err("second approval must fail");

    assert!(
        matches!(err, BusError::CommandDuplicate { .. }),
        "expected CommandDuplicate on second approval"
    );
}

// ─── Scenario D: clock drift > 60s → TimeDriftRejected ────────────────────────
#[test]
fn scenario_d_heartbeat_clock_drift_rejected() {
    let db = fresh_db();

    // Register an agent first.
    let reg = AgentRegistry::new(&db);
    let agent = AgentProcess::new("FuXiX", ProviderKind::Claude, "ryanx-main");
    reg.register(&agent).unwrap();

    let tracker = HeartbeatTracker::new(&db);

    // Simulate agent clock 90s ahead of server.
    let drifted_wall = Utc::now() + chrono::Duration::seconds(90);
    let err = tracker
        .record("FuXiX", drifted_wall, 0, None, None, None, None)
        .expect_err("should reject drift");

    // Spec says >60s drift is rejected; actual skew may round to 89999ms due to timing.
    assert!(
        matches!(err, BusError::TimeDriftRejected { skew_ms } if skew_ms.abs() > 60_000),
        "expected TimeDriftRejected with >60s drift, got: {err:?}"
    );

    // Verify a TimeDriftRejected event was emitted.
    let count: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM event_log WHERE event_type LIKE '%TimeDriftRejected%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "one TimeDriftRejected event in log");
}

// ─── Scenario E: event_log tamper → chain break → RED_STOP ───────────────────
#[test]
fn scenario_e_tampered_event_log_triggers_red_stop() {
    let db = fresh_db();
    let log = EventLog::new(&db);

    // Append 3 events.
    for i in 0..3u32 {
        log.append(
            EventType::AgentRegistered,
            Some("ryanx"),
            Some(&format!("agent_{i}")),
            None,
            None,
            serde_json::json!({ "i": i }),
        )
        .unwrap();
    }

    // Chain should verify cleanly.
    let n = log.verify_chain(100).expect("clean chain");
    assert_eq!(n, 3);

    // Tamper: overwrite the hash of the second event.
    db.conn
        .execute(
            "UPDATE event_log SET event_hash = 'deadbeef00000000' WHERE rowid = 2",
            [],
        )
        .unwrap();

    // Now verification must detect the break.
    let err = log.verify_chain(100).expect_err("should detect tamper");
    assert!(
        matches!(err, BusError::ChainBroken { .. }),
        "expected ChainBroken, got: {err:?}"
    );
}

#[test]
fn scenario_e_startup_verify_sets_red_stop() {
    let db = fresh_db();
    let log = EventLog::new(&db);

    // Append a few events.
    for i in 0..5u32 {
        log.append(
            EventType::AgentRegistered,
            Some("ryanx"),
            Some(&format!("agent_{i}")),
            None,
            None,
            serde_json::json!({}),
        )
        .unwrap();
    }

    // Tamper event #3.
    db.conn
        .execute(
            "UPDATE event_log SET event_hash = 'ffffffffffffffff' WHERE rowid = 3",
            [],
        )
        .unwrap();

    // Startup verify should activate RED_STOP.
    let result = log.startup_verify();
    assert!(result.is_err(), "startup_verify should fail on tampered chain");

    // RED_STOP must now be set.
    assert!(db.is_red_stop().unwrap(), "RED_STOP should be active");

    // All mutation commands must be rejected.
    let queue = CommandQueue::new(&db);
    let err = queue
        .enqueue(
            CommandSource::Internal,
            "internal_1",
            "ryanx",
            serde_json::json!({ "action": "test" }),
            RiskClass::ReadOnly,
        )
        .expect_err("must reject during RED_STOP");

    assert!(
        matches!(err, BusError::RedStop { .. }),
        "expected RedStop error, got {err:?}"
    );
}

// ─── Lease / T1 single-writer invariant ───────────────────────────────────────
#[test]
fn telegram_poller_single_writer_lease() {
    let db = fresh_db();
    let mgr = LeaseManager::new(&db);

    // First holder acquires poller lease.
    let lease_id = mgr
        .acquire(LeaseScope::TelegramPoller, "poller-1", TELEGRAM_POLLER_LEASE_TTL_SECS)
        .expect("first acquire");

    // Second holder is rejected.
    let err = mgr
        .acquire(LeaseScope::TelegramPoller, "poller-2", TELEGRAM_POLLER_LEASE_TTL_SECS)
        .expect_err("second acquire must fail");

    assert!(
        matches!(err, BusError::LeaseNotHeld { .. }),
        "expected LeaseNotHeld, got {err:?}"
    );

    // After revoke, second holder can acquire.
    mgr.revoke(lease_id).unwrap();
    mgr.acquire(LeaseScope::TelegramPoller, "poller-2", TELEGRAM_POLLER_LEASE_TTL_SECS)
        .expect("acquire after revoke");
}
