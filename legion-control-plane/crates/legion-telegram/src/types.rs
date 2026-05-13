/// Telegram Bot API domain types (receive path only for Phase 2).

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub from: Option<User>,
    pub chat: Chat,
    pub text: Option<String>,
    pub date: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
    pub username: Option<String>,
    pub first_name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub id: i64,
    pub username: Option<String>,
    pub first_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub from: User,
    pub data: Option<String>,
    pub message: Option<Message>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BotInfo {
    pub id: i64,
    pub username: String,
    pub first_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WebhookInfo {
    pub url: String,
    pub pending_update_count: u32,
}

#[derive(Debug, Clone)]
pub struct SentMessage {
    pub message_id: i64,
}
