use legion_bus::BusDb;

pub fn render(db: &BusDb) -> String {
    let mut stmt = match db.conn.prepare(
        "SELECT task_id, title, assignee, status, updated_at
           FROM task_orders
          WHERE status NOT IN ('done', 'cancelled')
          ORDER BY updated_at DESC
          LIMIT 20",
    ) {
        Ok(s) => s,
        Err(e) => return format!("DB error: {e}"),
    };

    let rows: Vec<(String, String, Option<String>, String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
        .map(|iter| iter.flatten().collect())
        .unwrap_or_default();

    if rows.is_empty() {
        return "No active tasks.".into();
    }

    let mut out = format!("=== Active Tasks ({}) ===\n", rows.len());
    for (id, title, assignee, status, updated) in &rows {
        let short_id = id.get(..8).unwrap_or(id);
        let agent = assignee.as_deref().unwrap_or("unassigned");
        let ts = updated.get(..16).unwrap_or(updated);
        out.push_str(&format!(
            "• {short_id}… {title}\n  [{status}] → {agent}  @{ts}\n"
        ));
    }
    out
}
