use legion_bus::BusDb;

pub fn render(db: &BusDb) -> String {
    let mut stmt = match db.conn.prepare(
        "SELECT agent_id, provider, state, updated_at FROM agent_registry ORDER BY agent_id",
    ) {
        Ok(s) => s,
        Err(e) => return format!("DB error: {e}"),
    };

    let rows: Vec<(String, String, String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map(|iter| iter.flatten().collect())
        .unwrap_or_default();

    if rows.is_empty() {
        return "No agents registered.".into();
    }

    let mut out = format!("=== Agents ({}) ===\n", rows.len());
    for (id, provider, state, updated) in &rows {
        // state is JSON-encoded (e.g. `"online"` with quotes) — strip them.
        let state_clean = state.trim_matches('"');
        let ts = updated.get(..16).unwrap_or(updated);
        out.push_str(&format!("• {id}  [{state_clean}]  {provider}  @{ts}\n"));
    }
    out
}
