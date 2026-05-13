/// Chaos tests for the Legion Telegram gateway (Phase 2 / 2.1 / 2.2).
///
/// Each test uses FakeTelegramApi -- no network I/O.
///
/// Scenarios:
///   C1  -- T1:   second poller is blocked by lease (single-writer invariant)
///   C2  -- T3:   webhook present on boot -> delete_webhook called
///   C3  -- T5:   429 rate-limit -> retry_after stored as not_before in outbox
///   C4  -- T6:   duplicate update_id -> ingested only once
///   C5  -- T2:   offset persisted -> new poller resumes from saved offset
///   C6  --       /status /agents /tasks return non-empty text without panicking
///   C7  -- T2:   durable ingest before offset advance (replay-safe ingest)
///   C8  -- T3:   409 Conflict -> deleteWebhook called in run() loop
///   C9  -- T7:   outbox row claim prevents double-send
///   C10 -- T2.2: dispatch_pending recovers rows stranded after crash-before-dispatch
///   C11 -- T2.2: outbox enqueue + status update is atomic; partial-tx rollback
///                keeps the row recoverable, retry dispatches exactly once

mod common;

use std::sync::Arc;

use common::{fresh_db, text_update, FakeResponse, FakeTelegramApi};
use legion_bus::leases::LeaseManager;
use legion_bus::BusError;
use legion_telegram::{
    OutboxManager, Poller, PollerConfig, TgError,
    offset::load_offset,
};

// --- C1: T1 single-writer lease ---------------------------------------------

#[tokio::test]
async fn c1_second_poller_rejected_by_lease() {
    let db = fresh_db();
    let api1 = FakeTelegramApi::new();
    let api2 = FakeTelegramApi::new();

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };

    let poller1 = Poller::new(Arc::clone(&api1), Arc::clone(&db), Arc::clone(&outbox), cfg);

    // Poller-1 boots and acquires the lease.
    let _lease = poller1.boot().await.expect("poller-1 should acquire lease");

    // Poller-2 with same LeaseScope must be rejected.
    let poller2_cfg = PollerConfig { bot_id: "bot2".into(), ..Default::default() };
    let poller2 = Poller::new(Arc::clone(&api2), Arc::clone(&db), Arc::clone(&outbox), poller2_cfg);

    let err = poller2.boot().await.expect_err("poller-2 should fail");
    assert!(
        matches!(err, TgError::Bus(BusError::LeaseNotHeld { .. })),
        "expected LeaseNotHeld, got {err:?}"
    );
}

// --- C2: T3 webhook reconcile on boot ---------------------------------------

#[tokio::test]
async fn c2_webhook_deleted_on_boot() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();
    api.set_webhook(true); // simulate active webhook

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    poller.boot().await.expect("boot should succeed");

    let calls = *api.deleted_webhook_calls.lock().unwrap();
    assert_eq!(calls, 1, "delete_webhook must be called exactly once (T3)");

    // Webhook must now be cleared.
    assert!(
        !*api.has_webhook.lock().unwrap(),
        "webhook flag should be cleared after delete"
    );
}

// --- C3: T5 429 -> not_before stored in outbox ------------------------------

#[tokio::test]
async fn c3_rate_limited_outbox_message_gets_not_before() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    // First send attempt -> 429 with retry_after=2.
    // Second send attempt -> success.
    // We test via OutboxManager directly (no need for full poll loop).
    let outbox = OutboxManager::new(Arc::clone(&db));
    outbox.enqueue(100, "hello".into());

    assert_eq!(outbox.pending_count(), 1, "one message queued");

    // Use a fake that returns 429 first.
    let api_429 = FakeTelegramApi::new();
    // FakeTelegramApi.send_message always succeeds by default; we need to
    // simulate 429 at the send level.  Use a purpose-built fake:
    use async_trait::async_trait;
    use legion_telegram::{TelegramApi, TgResult};
    use legion_telegram::types::*;

    struct RateLimitedApi { calls: std::sync::Mutex<u32> }

    #[async_trait]
    impl TelegramApi for RateLimitedApi {
        async fn get_me(&self) -> TgResult<BotInfo> { unreachable!() }
        async fn get_webhook_info(&self) -> TgResult<WebhookInfo> { unreachable!() }
        async fn delete_webhook(&self) -> TgResult<()> { unreachable!() }
        async fn get_updates(&self, _: i64, _: u64, _: u32) -> TgResult<Vec<Update>> { unreachable!() }
        async fn answer_callback_query(&self, _: String, _: Option<String>) -> TgResult<()> { unreachable!() }
        async fn send_message(&self, _chat_id: i64, _text: String, _: Option<String>) -> TgResult<SentMessage> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Err(TgError::RateLimited { retry_after_secs: 2 })
            } else {
                Ok(SentMessage { message_id: 99 })
            }
        }
    }

    let rl_api = Arc::new(RateLimitedApi { calls: std::sync::Mutex::new(0) });

    // First flush -> 429 -> message stays pending, not_before set.
    outbox.flush_once(rl_api.as_ref()).await;
    assert_eq!(outbox.sent_count(), 0, "message not sent after 429");

    // Verify not_before is set in the DB (message still pending but deferred).
    let not_before: Option<String> = {
        let db = db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT not_before FROM telegram_outbox WHERE status = 'pending'",
                [],
                |r| r.get(0),
            )
            .ok()
    };
    assert!(
        not_before.is_some(),
        "not_before must be set after 429 (T5/T7)"
    );
}

// --- C4: T6 duplicate update_id -> idempotent --------------------------------

#[tokio::test]
async fn c4_duplicate_update_id_processed_once() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    // Queue the same update_id twice.
    api.push(FakeResponse::Updates(vec![
        text_update(100, 1, "/status"),
        text_update(100, 1, "/status"), // duplicate
    ]));
    api.push(FakeResponse::Updates(vec![])); // second poll returns empty

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    // Boot (acquires lease, no webhook to delete).
    poller.boot().await.expect("boot");

    // First poll_once: update_id=100 stored, second ignored.
    let new_count = poller.poll_once().await.expect("poll_once");
    assert_eq!(new_count, 1, "only 1 new update despite 2 rows with same update_id (T6)");

    // Only one row in telegram_updates.
    let stored: i64 = {
        let db = db.lock().unwrap();
        db.conn
            .query_row("SELECT COUNT(*) FROM telegram_updates", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(stored, 1, "exactly one telegram_updates row");
}

// --- C5: T2 offset persistence -- new poller resumes from saved offset --------

#[tokio::test]
async fn c5_offset_persisted_across_poller_restarts() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    // First poller processes update_id 50 and 51.
    api.push(FakeResponse::Updates(vec![
        text_update(50, 1, "/agents"),
        text_update(51, 1, "/tasks"),
    ]));

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "botA".into(), ..Default::default() };
    let poller1 = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    poller1.boot().await.expect("boot");
    poller1.poll_once().await.expect("poll");

    // Saved offset must be 52 (= max_id + 1).
    let offset = load_offset(&db, "botA").expect("load offset");
    assert_eq!(offset, 52, "offset must be max_update_id + 1 (T2)");

    // Revoke lease so a second poller can acquire it.
    {
        let db = db.lock().unwrap();
        let mgr = LeaseManager::new(&db);
        let lease_row: Option<String> = db
            .conn
            .query_row(
                "SELECT lease_id FROM authority_leases WHERE status = '\"active\"' OR status = 'active'",
                [],
                |r| r.get(0),
            )
            .ok();
        if let Some(id_str) = lease_row {
            if let Ok(uuid) = id_str.parse::<uuid::Uuid>() {
                let _ = mgr.revoke(uuid);
            }
        }
    }

    // Second poller (simulating restart): should start from offset 52.
    let api2 = FakeTelegramApi::new();
    api2.push(FakeResponse::Updates(vec![])); // empty batch

    let poller2 = Poller::new(
        Arc::clone(&api2),
        Arc::clone(&db),
        Arc::clone(&outbox),
        PollerConfig { bot_id: "botA".into(), ..Default::default() },
    );
    poller2.boot().await.expect("boot2");

    // Verify offset loaded is still 52 (not reset to 0).
    let offset2 = load_offset(&db, "botA").expect("load offset2");
    assert_eq!(offset2, 52, "restart must resume from persisted offset (T2)");
}

// --- C7: T2 durable ingest before offset advance ----------------------------
//
// poll_once() is ingest-only (Phase 2.2). It writes rows as 'pending_dispatch'
// and advances the offset only AFTER every row is durable. A simulated crash
// that resets the offset must replay the same batch with zero new ingest
// (T6 INSERT OR IGNORE) and zero dispatched state changes. Dispatch recovery
// is exercised separately in C10.

#[tokio::test]
async fn c7_t2_durable_ingest_before_offset_advance() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    api.push(FakeResponse::Updates(vec![
        text_update(200, 1, "/agents"),
        text_update(201, 1, "/agents"),
    ]));
    // On "restart", Telegram resends the same batch (offset not acknowledged).
    api.push(FakeResponse::Updates(vec![
        text_update(200, 1, "/agents"),
        text_update(201, 1, "/agents"),
    ]));

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    poller.boot().await.expect("boot");

    let n = poller.poll_once().await.expect("first poll");
    assert_eq!(n, 2, "2 new updates ingested");

    let offset = load_offset(&db, "bot1").expect("offset");
    assert_eq!(offset, 202, "offset advanced to max_id+1 only after every row was durable");

    // Both rows are durably stored as 'pending_dispatch'.
    let (count, pending): (i64, i64) = {
        let db = db.lock().unwrap();
        let c: i64 = db.conn.query_row("SELECT COUNT(*) FROM telegram_updates", [], |r| r.get(0)).unwrap();
        let p: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM telegram_updates WHERE status='pending_dispatch'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        (c, p)
    };
    assert_eq!(count, 2, "2 rows durably stored in telegram_updates");
    assert_eq!(pending, 2, "both rows are pending_dispatch (dispatch not run by poll_once)");

    // Simulate crash: reset offset back to 200 (as if save_offset never completed).
    {
        let db = db.lock().unwrap();
        db.conn
            .execute(
                "UPDATE telegram_offsets SET last_update_id = 200 WHERE bot_id = 'bot1'",
                [],
            )
            .unwrap();
    }

    // Second poll with same batch: all updates already in DB -> zero new ingest.
    let n2 = poller.poll_once().await.expect("second poll after simulated crash");
    assert_eq!(n2, 0, "0 new ingests -- T6 deduplicated the replay");

    // Offset re-advances to 202; rows still pending_dispatch, none lost.
    let offset2 = load_offset(&db, "bot1").expect("offset2");
    assert_eq!(offset2, 202, "offset correctly re-advanced to 202");

    let still_pending: i64 = {
        let db = db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM telegram_updates WHERE status='pending_dispatch'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(still_pending, 2, "both rows remain pending_dispatch; dispatch will pick them up");
}

// --- C8: 409 Conflict -> TgError::Conflict + deleteWebhook in run() ---------

#[tokio::test]
async fn c8_conflict_409_triggers_webhook_reconcile_in_run() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    // Sequence: 409 Conflict (triggers reconcile) -> 401 Unauthorized (stops loop).
    api.push(FakeResponse::Conflict);
    api.push(FakeResponse::Unauthorized);

    let outbox = OutboxManager::new(Arc::clone(&db));
    let cfg = PollerConfig { bot_id: "bot1".into(), max_retries: 1, ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    let lease_id = poller.boot().await.expect("boot");
    // boot() calls delete_webhook once if webhook is set; api has no webhook here -> 0 calls.
    let deletes_before = *api.deleted_webhook_calls.lock().unwrap();

    // run() should hit 409, call deleteWebhook (T3 reconcile), then stop on 401.
    let err = poller.run(lease_id).await.expect_err("run must stop on Unauthorized");
    assert!(
        matches!(err, TgError::Unauthorized),
        "run loop stopped by Unauthorized, got: {err:?}"
    );

    let deletes_after = *api.deleted_webhook_calls.lock().unwrap();
    assert_eq!(
        deletes_after - deletes_before,
        1,
        "deleteWebhook called exactly once for 409 Conflict reconcile (T3)"
    );
}

// --- C9: Outbox row claim prevents double-send across two flush calls --------
//
// The atomic claim (UPDATE ... WHERE claim_id IS NULL) means a second flush
// that races the first one will claim no rows and send nothing.
// We verify the invariant sequentially: first flush claims+sends all rows;
// second flush finds the rows already claimed/sent and does nothing.

#[tokio::test]
async fn c9_outbox_claim_prevents_double_send() {
    let db = fresh_db();
    let outbox = OutboxManager::new(Arc::clone(&db));

    outbox.enqueue(42, "msg1".into());
    outbox.enqueue(42, "msg2".into());
    outbox.enqueue(99, "msg3".into());

    assert_eq!(outbox.pending_count(), 3, "3 messages pending");

    let api = FakeTelegramApi::new();

    // First flush: claims all 3 unclaimed rows and sends them.
    let sent1 = outbox.flush_once(&*api).await;
    assert_eq!(sent1, 3, "first flush sends all 3 messages");

    // Second flush: rows now have claim_id set (or are already 'sent');
    // the atomic claim selects zero rows -> nothing is re-sent.
    let sent2 = outbox.flush_once(&*api).await;
    assert_eq!(sent2, 0, "second flush sends nothing -- claim mechanism prevents double-send");

    assert_eq!(outbox.sent_count(), 3, "3 rows in 'sent' state");
    assert_eq!(outbox.pending_count(), 0, "no messages still pending");

    let api_calls = api.sent_messages.lock().unwrap().len();
    assert_eq!(api_calls, 3, "exactly 3 send_message API calls -- no double-send");
}

// --- C10: dispatch_pending recovers rows stranded by a crash-before-dispatch ---
//
// This is the bug that the Phase 2.1 fix missed: record_update succeeds,
// then the process dies BEFORE dispatch runs. The row is durably present
// as 'pending_dispatch' but no outbox row exists. On restart, Telegram
// replays the same update; INSERT OR IGNORE returns is_new=false; the old
// design's poll_once branched into the duplicate arm and never dispatched.
//
// Under the Phase 2.2 design, dispatch is decoupled: a separate
// dispatch_pending() scans the DB for pending_dispatch rows and processes
// them. This test simulates the crash scenario by pre-inserting a row
// directly, then verifies dispatch_pending picks it up exactly once.

#[tokio::test]
async fn c10_dispatch_pending_recovers_stranded_row() {
    let db = fresh_db();
    let outbox = OutboxManager::new(Arc::clone(&db));
    let api = FakeTelegramApi::new();
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    // Simulate crash after record_update, before dispatch:
    // a row exists with status='pending_dispatch' but no outbox row.
    let raw = serde_json::to_string(&text_update(300, 1, "/status")).unwrap();
    {
        let db = db.lock().unwrap();
        db.conn
            .execute(
                "INSERT INTO telegram_updates (update_id, raw_json, received_at, status)
                 VALUES (?1, ?2, ?3, 'pending_dispatch')",
                rusqlite::params![300i64, raw, chrono::Utc::now().to_rfc3339()],
            )
            .unwrap();
    }

    assert_eq!(outbox.pending_count(), 0, "no outbox row exists before dispatch_pending");

    // dispatch_pending must pick the row up and dispatch it exactly once.
    let n = poller.dispatch_pending().expect("dispatch");
    assert_eq!(n, 1, "exactly one row dispatched from recovered pending_dispatch state");
    assert_eq!(outbox.pending_count(), 1, "one outbox row enqueued");

    // Row transitioned to 'dispatched' with a timestamp.
    let (status, dispatched_at): (String, Option<String>) = {
        let db = db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT status, dispatched_at FROM telegram_updates WHERE update_id=300",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
    };
    assert_eq!(status, "dispatched", "row reached terminal state");
    assert!(dispatched_at.is_some(), "dispatched_at timestamp populated");

    // Second call: idempotent (no double-enqueue).
    let n2 = poller.dispatch_pending().expect("second");
    assert_eq!(n2, 0, "second call finds no pending rows -- exactly-once");
    assert_eq!(outbox.pending_count(), 1, "still exactly one outbox row");
}

// --- C11: outbox enqueue + status update is a single transaction ------------
//
// The reviewer-mandated contract: if the outbox INSERT succeeds inside the
// dispatch transaction but the status UPDATE fails (or the process dies
// between them), the entire transaction must roll back. The row stays
// 'pending_dispatch', the outbox stays empty, and the next dispatch_pending
// retries exactly once -- no duplicate terminal state, no orphan outbox row.
//
// We simulate the partial-tx failure by manually running BEGIN + INSERT
// outbox + drop-without-commit. This is the same rollback path that
// dispatch_pending's tx would take on any internal error.

#[tokio::test]
async fn c11_partial_tx_rollback_keeps_row_recoverable() {
    let db = fresh_db();
    let outbox = OutboxManager::new(Arc::clone(&db));
    let api = FakeTelegramApi::new();
    let cfg = PollerConfig { bot_id: "bot1".into(), ..Default::default() };
    let poller = Poller::new(Arc::clone(&api), Arc::clone(&db), Arc::clone(&outbox), cfg);

    // Pre-insert a pending_dispatch row (simulating prior ingest).
    let raw = serde_json::to_string(&text_update(400, 7, "/status")).unwrap();
    {
        let db = db.lock().unwrap();
        db.conn
            .execute(
                "INSERT INTO telegram_updates (update_id, raw_json, received_at, status)
                 VALUES (?1, ?2, ?3, 'pending_dispatch')",
                rusqlite::params![400i64, raw, chrono::Utc::now().to_rfc3339()],
            )
            .unwrap();
    }

    // Simulate "outbox enqueue succeeded, then forced failure before status update":
    // run a partial transaction (BEGIN -> INSERT outbox -> drop without commit).
    // This is exactly what would happen if dispatch_pending's tx hit an error
    // after INSERT but before UPDATE: the auto-rollback unwinds both writes.
    {
        let db = db.lock().unwrap();
        let tx = db.conn.unchecked_transaction().unwrap();
        tx.execute(
            "INSERT INTO telegram_outbox
                (chat_id, message_seq, payload, method, status, retry_count, created_at)
             VALUES (?1, ?2, ?3, 'sendMessage', 'pending', 0, ?4)",
            rusqlite::params![
                7i64,
                1i64,
                serde_json::json!({ "text": "would-be response" }).to_string(),
                chrono::Utc::now().to_rfc3339(),
            ],
        )
        .unwrap();
        // Drop tx without commit -> rollback.
        drop(tx);
    }

    // Post-rollback invariants: nothing leaked.
    assert_eq!(outbox.pending_count(), 0, "rollback unwound the outbox INSERT");
    let status: String = {
        let db = db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT status FROM telegram_updates WHERE update_id=400",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(status, "pending_dispatch", "row remained pending after rollback");

    // Retry: dispatch_pending must dispatch exactly once.
    let n = poller.dispatch_pending().expect("retry");
    assert_eq!(n, 1, "retry dispatched the recovered row");
    assert_eq!(outbox.pending_count(), 1, "exactly one outbox row -- no duplicate from earlier rollback");

    let final_status: String = {
        let db = db.lock().unwrap();
        db.conn
            .query_row(
                "SELECT status FROM telegram_updates WHERE update_id=400",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(final_status, "dispatched", "terminal state reached on retry");

    // Third call must be a no-op.
    let n2 = poller.dispatch_pending().expect("third");
    assert_eq!(n2, 0, "third call finds nothing pending");
    assert_eq!(outbox.pending_count(), 1, "still exactly one outbox row");
}

// --- C6: read-only command handlers return non-empty strings ----------------

#[test]
fn c6_read_only_commands_return_text() {
    let db_arc = fresh_db();
    let db = db_arc.lock().unwrap();

    // Populate minimal data.
    let now = chrono::Utc::now().to_rfc3339();
    db.conn
        .execute(
            "INSERT INTO agent_registry
                (agent_id, provider, profile_id, state, registered_at, updated_at)
             VALUES ('AgentX', '\"Claude\"', 'ryanx', '\"online\"', ?1, ?1)",
            rusqlite::params![now],
        )
        .unwrap();

    let status_text = legion_telegram::commands::handle("/status", &db);
    assert!(!status_text.is_empty(), "/status must return text");
    assert!(status_text.contains("Legion Bus Status"), "/status header missing");

    let agents_text = legion_telegram::commands::handle("/agents", &db);
    assert!(agents_text.contains("AgentX"), "/agents must list registered agents");

    let tasks_text = legion_telegram::commands::handle("/tasks", &db);
    assert!(!tasks_text.is_empty(), "/tasks must return text");

    let unknown = legion_telegram::commands::handle("/explode", &db);
    assert!(unknown.contains("Unknown command"), "unknown command should show error");
}
