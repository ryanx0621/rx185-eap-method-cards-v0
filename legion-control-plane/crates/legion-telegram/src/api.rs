use async_trait::async_trait;

use crate::error::{TgError, TgResult};
use crate::types::*;

// ─── Public trait ───────────────────────────────────────────────────────────

/// Abstraction over the Telegram Bot API HTTP calls.
///
/// All poller and outbox logic calls through this trait, making it trivial to
/// inject `FakeTelegramApi` in chaos tests without network I/O.
#[async_trait]
pub trait TelegramApi: Send + Sync + 'static {
    async fn get_me(&self) -> TgResult<BotInfo>;
    async fn get_webhook_info(&self) -> TgResult<WebhookInfo>;
    async fn delete_webhook(&self) -> TgResult<()>;

    /// Long-poll getUpdates (T4: `timeout=25`, HTTP read timeout must be >25s).
    async fn get_updates(&self, offset: i64, timeout: u64, limit: u32)
        -> TgResult<Vec<Update>>;

    async fn send_message(
        &self,
        chat_id: i64,
        text: String,
        parse_mode: Option<String>,
    ) -> TgResult<SentMessage>;

    async fn answer_callback_query(
        &self,
        callback_query_id: String,
        text: Option<String>,
    ) -> TgResult<()>;
}

// ─── Real HTTP implementation (feature = "http") ─────────────────────────────

#[cfg(feature = "http")]
pub use real::ReqwestClient;

#[cfg(feature = "http")]
mod real {
    use super::*;
    use reqwest::Client;

    pub struct ReqwestClient {
        client: Client,
        base: String,
    }

    impl ReqwestClient {
        /// `read_timeout_secs` must exceed the long-poll `timeout` param (T4).
        pub fn new(token: impl Into<String>, read_timeout_secs: u64) -> Self {
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(read_timeout_secs))
                .build()
                .expect("reqwest TLS init failed");
            let token = token.into();
            Self {
                client,
                base: format!("https://api.telegram.org/bot{token}"),
            }
        }

        async fn call<T: serde::de::DeserializeOwned>(
            &self,
            method: &str,
            body: serde_json::Value,
        ) -> TgResult<T> {
            let url = format!("{}/{method}", self.base);
            let resp = self
                .client
                .post(&url)
                .json(&body)
                .send()
                .await
                .map_err(|e| TgError::Network(e.to_string()))?;

            let status = resp.status();
            if status == 401 {
                return Err(TgError::Unauthorized);
            }
            if status == 404 {
                return Err(TgError::NotFound);
            }
            if status.as_u16() == 409 {
                return Err(TgError::Conflict);
            }
            if status.as_u16() == 429 {
                let retry_after = resp
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| v["parameters"]["retry_after"].as_u64())
                    .unwrap_or(1);
                return Err(TgError::RateLimited { retry_after_secs: retry_after });
            }
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(TgError::Http { status: status.as_u16(), body: body_text });
            }

            let json: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| TgError::Parse(e.to_string()))?;

            if !json["ok"].as_bool().unwrap_or(false) {
                let desc = json["description"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                return Err(TgError::Http { status: status.as_u16(), body: desc });
            }

            serde_json::from_value::<T>(json["result"].clone())
                .map_err(|e| TgError::Parse(e.to_string()))
        }
    }

    #[async_trait]
    impl TelegramApi for ReqwestClient {
        async fn get_me(&self) -> TgResult<BotInfo> {
            self.call("getMe", serde_json::json!({})).await
        }

        async fn get_webhook_info(&self) -> TgResult<WebhookInfo> {
            self.call("getWebhookInfo", serde_json::json!({})).await
        }

        async fn delete_webhook(&self) -> TgResult<()> {
            let _: serde_json::Value = self
                .call("deleteWebhook", serde_json::json!({ "drop_pending_updates": false }))
                .await?;
            Ok(())
        }

        async fn get_updates(
            &self,
            offset: i64,
            timeout: u64,
            limit: u32,
        ) -> TgResult<Vec<Update>> {
            self.call(
                "getUpdates",
                serde_json::json!({
                    "offset": offset,
                    "timeout": timeout,
                    "limit": limit,
                    "allowed_updates": ["message", "callback_query"],
                }),
            )
            .await
        }

        async fn send_message(
            &self,
            chat_id: i64,
            text: String,
            parse_mode: Option<String>,
        ) -> TgResult<SentMessage> {
            let mut body = serde_json::json!({ "chat_id": chat_id, "text": text });
            if let Some(pm) = parse_mode {
                body["parse_mode"] = serde_json::Value::String(pm);
            }
            let msg: Message = self.call("sendMessage", body).await?;
            Ok(SentMessage { message_id: msg.message_id })
        }

        async fn answer_callback_query(
            &self,
            callback_query_id: String,
            text: Option<String>,
        ) -> TgResult<()> {
            let mut body =
                serde_json::json!({ "callback_query_id": callback_query_id });
            if let Some(t) = text {
                body["text"] = serde_json::Value::String(t);
            }
            let _: serde_json::Value = self.call("answerCallbackQuery", body).await?;
            Ok(())
        }
    }
}
