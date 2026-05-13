/// Read-only Telegram command dispatcher (Phase 2).
///
/// Accepts `/status`, `/agents`, `/tasks` only.
/// All responses are plain text formatted for mobile.

mod agents;
mod status;
mod tasks;

use legion_bus::BusDb;

/// Dispatch a `/command [args]` string and return the reply text.
///
/// Unknown commands return a help message. No mutations are performed here.
pub fn handle(text: &str, db: &BusDb) -> String {
    let mut parts = text.splitn(2, ' ');
    let cmd = parts.next().unwrap_or("").to_ascii_lowercase();
    let _args = parts.next().unwrap_or("").trim();

    match cmd.as_str() {
        "/status"  => status::render(db),
        "/agents"  => agents::render(db),
        "/tasks"   => tasks::render(db),
        "/help"    => help_text(),
        _          => format!("Unknown command: {cmd}\n\n{}", help_text()),
    }
}

fn help_text() -> String {
    "Legion read-only commands:\n\
     /status — Bus health & RED_STOP flag\n\
     /agents — List all agents and their states\n\
     /tasks  — List active task orders"
        .into()
}
