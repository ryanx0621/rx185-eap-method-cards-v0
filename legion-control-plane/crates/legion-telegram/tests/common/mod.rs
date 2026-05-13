/// Shared test helpers: FakeTelegramApi and DB factory.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use legion_bus::BusDb;
use legion_telegram::{
    TelegramApi, TgError, TgResult,
    types::{BotInfo, Message, Chat, SentMessage, Update, User, WebhookInfo},
};

// ─── Canned responses ───────────────────────────────────────────────────────────

/// A pre-programmed response for `get_updates`.
#[derive(Clone)]
pub enum FakeResponse {
    /// Return these updates.
    Updates(Vec<Update>),
    /// Simulate 429 with given retry_after.
    RateLimited(u64),
    /// Simulate a transient network error.
    NetworkError,
    /// Simulate 401 (fatal).
    Unauthorized,
}

// ─── FakeTelegramApi ─────────────────────────────────────────────────────────

pub struct FakeTelegramApi {
    /// Queue of responses to return from `get_updates`, drained in order.
    pub responses: Mutex<VecDeque<FakeResponse>>,
    /// Whether the fake should report an active webhook on first get_webhook_info.
    pub has_webhook: Mutex<bool>,
    /// Calls recorded for assertion.
    pub deleted_webhook_calls: Mutex<u32>,
    pub sent_messages: Mutex<Vec<(i64, String)>>,
}

impl FakeTelegramApi {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(VecDeque::new()),
            has_webhook: Mutex::new(false),
            deleted_webhook_calls: Mutex::new(0),
            sent_messages: Mutex::new(Vec::new()),
        })
    }

    pub fn push(&self, r: FakeResponse) {
        self.responses.lock().unwrap().push_back(r);
    }

    pub fn set_webhook(&self, active: bool) {
        *self.has_webhook.lock().unwrap() = active;
    }
}

#[async_trait]
impl TelegramApi for FakeTelegramApi {
    async fn get_me(&self) -> TgResult<BotInfo> {
        Ok(BotInfo { id: 42, username: "LegionBot".into(), first_name: "Legion".into() })
    }

    async fn get_webhook_info(&self) -> TgResult<WebhookInfo> {
        let url = if *self.has_webhook.lock().unwrap() {
            "https://evil.example.com/hook".into()
        } else {
            String::new()
        };
        Ok(WebhookInfo { url, pending_update_count: 0 })
    }

    async fn delete_webhook(&self) -> TgResult<()> {
        *self.deleted_webhook_calls.lock().unwrap() += 1;
        *self.has_webhook.lock().unwrap() = false;
        Ok(())
    }

    async fn get_updates(
        &self,
        _offset: i64,
        _timeout: u64,
        _limit: u32,
    ) -> TgResult<Vec<Update>> {
        match self.responses.lock().unwrap().pop_front() {
            Some(FakeResponse::Updates(v)) => Ok(v),
            Some(FakeResponse::RateLimited(secs)) => {
                Err(TgError::RateLimited { retry_after_secs: secs })
            }
            Some(FakeResponse::NetworkError) => {
                Err(TgError::Network("simulated network failure".into()))
            }
            Some(FakeResponse::Unauthorized) => Err(TgError::Unauthorized),
            None => Ok(vec![]), // empty batch — simulates long-poll timeout
        }
    }

    async fn send_message(
        &self,
        chat_id: i64,
        text: String,
        _parse_mode: Option<String>,
    ) -> TgResult<SentMessage> {
        self.sent_messages.lock().unwrap().push((chat_id, text));
        Ok(SentMessage { message_id: 1 })
    }

    async fn answer_callback_query(
        &self,
        _callback_query_id: String,
        _text: Option<String>,
    ) -> TgResult<()> {
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

pub fn fresh_db() -> Arc<Mutex<BusDb>> {
    Arc::new(Mutex::new(BusDb::open_in_memory().expect("in-memory db")))
}

/// Build a minimal text message update.
pub fn text_update(update_id: i64, chat_id: i64, text: &str) -> Update {
    Update {
        update_id,
        message: Some(Message {
            message_id: update_id,
            from: Some(User { id: 1, username: Some("ryanx".into()), first_name: "Ryan".into() }),
            chat: Chat {
                id: chat_id,
                chat_type: "private".into(),
                username: Some("ryanx".into()),
                first_name: Some("Ryan".into()),
            },
            text: Some(text.into()),
            date: 0,
        }),
        callback_query: None,
    }
}
