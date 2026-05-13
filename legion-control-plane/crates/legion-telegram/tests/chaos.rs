/// Chaos tests for the Legion Telegram gateway (Phase 2).
///
/// Each test uses FakeTelegramApi — no network I/O.
///
/// Scenarios:
///   C1 — T1: second poller is blocked by lease (single-writer invariant)
///   C2 — T3: webhook present on boot → delete_webhook called
///   C3 — T5: 429 rate-limit → retry_after stored as not_before in outbox
///   C4 — T6: duplicate update_id → processed only once
///   C5 — T2: offset persisted → new poller resumes from saved offset
///   C6 — /status /agents /tasks return non-empty text without panicking

mod common;

use std::sync::Arc;

use common::{fresh_db, text_update, FakeResponse, FakeTelegramApi};
use legion_bus::leases::LeaseManager;
use legion_bus::BusError;
use legion_telegram::{
    OutboxManager, Poller, PollerConfig, TgError,
    offset::load_offset,
};

// ─── C1: T1 single-writer lease ──────────────────────────────────────────────────────

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

// ─── C2: T3 webhook reconcile on boot ─────────────────────────────────────────────────

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

// ─── C3: T5 429 → not_before stored in outbox ─────────────────────────────────────────

#[tokio::test]
async fn c3_rate_limited_outbox_message_gets_not_before() {
    let db = fresh_db();
    let api = FakeTelegramApi::new();

    // First send attempt → 429 with retry_after=2.
    // Second send attempt → success.
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

    // First flush → 429 → message stays pending, not_before set.
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

// ─── C4: T6 duplicate update_id → idempotent ───────────────────────────────────────────────

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

// ─── C5: T2 offset persistence — new poller resumes from saved offset ─────────

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

// ─── C6: read-only command handlers return non-empty strings ──────────────

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
