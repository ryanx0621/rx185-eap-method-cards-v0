use legion_bus::BusDb;

pub fn render(db: &BusDb) -> String {
    let red_stop = db.is_red_stop().unwrap_or(false);
    let (reason, at) = if red_stop {
        let row: Option<(Option<String>, Option<String>)> = db
            .conn
            .query_row(
                "SELECT red_stop_reason, red_stop_at FROM bus_state WHERE singleton = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        row.unwrap_or_default()
    } else {
        (None, None)
    };

    let (chain_at, chain_n): (Option<String>, Option<i64>) = db
        .conn
        .query_row(
            "SELECT last_chain_verify_at, chain_verified_events FROM bus_state WHERE singleton = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap_or_default();

    let agent_count: i64 = db
        .conn
        .query_row("SELECT COUNT(*) FROM agent_registry", [], |r| r.get(0))
        .unwrap_or(0);

    let pending_cmds: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM remote_commands WHERE status = 'queued'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let mut out = String::from("=== Legion Bus Status ===\n");
    if red_stop {
        out.push_str(&format!(
            "🔴 RED_STOP ACTIVE\nReason: {}\nAt: {}\n",
            reason.as_deref().unwrap_or("unknown"),
            at.as_deref().unwrap_or("unknown"),
        ));
    } else {
        out.push_str("🟢 Bus OK\n");
    }
    out.push_str(&format!("Agents registered: {agent_count}\n"));
    out.push_str(&format!("Pending commands:  {pending_cmds}\n"));
    if let Some(n) = chain_n {
        out.push_str(&format!(
            "Chain last verified: {n} events @ {}\n",
            chain_at.as_deref().unwrap_or("never")
        ));
    }
    out
}
