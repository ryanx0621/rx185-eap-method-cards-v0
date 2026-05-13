-- Legion Bus Core: initial schema
-- All tables use INTEGER rowid for SKIP LOCKED-style work queues.
-- WAL mode and synchronous=NORMAL are set at connection open, not here.

PRAGMA foreign_keys = ON;

-- ─── Agent Registry ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id          TEXT PRIMARY KEY,
    provider          TEXT NOT NULL,
    profile_id        TEXT NOT NULL,
    state             TEXT NOT NULL DEFAULT 'offline',
    pid               INTEGER,
    workspace         TEXT,
    model_hint        TEXT,
    current_task_id   TEXT,
    lease_id          TEXT,
    registered_at     TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

-- ─── Authority Leases ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS authority_leases (
    lease_id          TEXT PRIMARY KEY,
    scope             TEXT NOT NULL,       -- LeaseScope discriminant
    holder_id         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'active',
    ttl_secs          INTEGER NOT NULL,
    acquired_at       TEXT NOT NULL,
    expires_at        TEXT NOT NULL,
    last_renewed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leases_scope_holder
    ON authority_leases(scope, holder_id, status);

-- ─── Task Orders ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS task_orders (
    task_id               TEXT PRIMARY KEY,
    title                 TEXT NOT NULL,
    description           TEXT,
    creator               TEXT NOT NULL,
    assignee              TEXT,
    reviewers             TEXT NOT NULL DEFAULT '[]', -- JSON array
    committee             TEXT NOT NULL DEFAULT '[]', -- JSON array
    priority              TEXT NOT NULL DEFAULT 'normal',
    risk_level            TEXT NOT NULL DEFAULT 'low',
    status                TEXT NOT NULL DEFAULT 'queued',
    acceptance_criteria   TEXT NOT NULL DEFAULT '[]', -- JSON array
    heartbeat_interval_sec INTEGER NOT NULL DEFAULT 180,
    last_heartbeat_at     TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

-- ─── Remote Commands ──────────────────────────────────────────────────────
-- command_id has a UNIQUE constraint — this is the exactly-once gate.
-- Any duplicate insert raises SQLITE_CONSTRAINT_UNIQUE.
CREATE TABLE IF NOT EXISTS remote_commands (
    command_id        TEXT PRIMARY KEY,
    source            TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    actor_id          TEXT NOT NULL,
    intent            TEXT NOT NULL,      -- canonical JSON
    risk_class        TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'queued',
    audit_event_id    TEXT,
    review_id         TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    UNIQUE(command_id)                    -- belt-and-suspenders, mirrors PK
);
CREATE INDEX IF NOT EXISTS idx_commands_status
    ON remote_commands(status, created_at);

-- ─── Event Log (append-only, hash-chained) ────────────────────────────────
CREATE TABLE IF NOT EXISTS event_log (
    rowid             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    event_type        TEXT NOT NULL,
    actor_id          TEXT,
    agent_id          TEXT,
    task_id           TEXT,
    command_id        TEXT,
    payload           TEXT NOT NULL DEFAULT '{}',  -- JSON
    created_at        TEXT NOT NULL,
    prev_event_hash   TEXT NOT NULL DEFAULT '',
    event_hash        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_agent
    ON event_log(agent_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_task
    ON event_log(task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_command
    ON event_log(command_id);

-- ─── Heartbeats ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS heartbeats (
    heartbeat_id      TEXT PRIMARY KEY,
    agent_id          TEXT NOT NULL,
    task_id           TEXT,
    wall_ts           TEXT NOT NULL,
    mono_ns           INTEGER NOT NULL,
    agent_clock_skew_ms INTEGER NOT NULL,
    progress_note     TEXT,
    blocker           TEXT,
    next_action       TEXT,
    received_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_heartbeats_agent
    ON heartbeats(agent_id, received_at DESC);

-- ─── Telegram Updates ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telegram_updates (
    update_id         INTEGER PRIMARY KEY,  -- Telegram's own monotonic update_id
    raw_json          TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'received'  -- received|parsed|deduplicated|rejected
);

-- Persisted poller offset: resume from here after restart (T2).
CREATE TABLE IF NOT EXISTS telegram_offsets (
    bot_id            TEXT PRIMARY KEY,
    last_update_id    INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL
);

-- ─── Telegram Outbox ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telegram_outbox (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id           INTEGER NOT NULL,
    message_seq       INTEGER NOT NULL,    -- per-chat ordering
    payload           TEXT NOT NULL,       -- JSON for sendMessage/editMessageText/etc.
    method            TEXT NOT NULL DEFAULT 'sendMessage',
    status            TEXT NOT NULL DEFAULT 'pending',   -- pending|sent|dead_letter
    retry_count       INTEGER NOT NULL DEFAULT 0,
    not_before        TEXT,                -- honor 429 retry_after
    created_at        TEXT NOT NULL,
    sent_at           TEXT,
    dead_lettered_at  TEXT,
    error_detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON telegram_outbox(status, not_before, chat_id, message_seq)
    WHERE status = 'pending';

-- ─── Callback Tokens ──────────────────────────────────────────────────────
-- Single-use HMAC tokens for inline approval buttons (spec §7).
CREATE TABLE IF NOT EXISTS callback_tokens (
    nonce             TEXT PRIMARY KEY,
    actor_id          TEXT NOT NULL,
    review_id         TEXT NOT NULL,
    command_kind      TEXT NOT NULL,
    target_id         TEXT NOT NULL,
    expires_at        TEXT NOT NULL,
    hmac              TEXT NOT NULL,
    used_at           TEXT               -- NULL = available; set on first use
);

-- ─── Bus State ────────────────────────────────────────────────────────────
-- Single-row table for global Bus flags such as RED_STOP.
CREATE TABLE IF NOT EXISTS bus_state (
    singleton         INTEGER PRIMARY KEY DEFAULT 1 CHECK(singleton = 1),
    red_stop          INTEGER NOT NULL DEFAULT 0,   -- 1 = all mutations blocked
    red_stop_reason   TEXT,
    red_stop_at       TEXT,
    last_chain_verify_at TEXT,
    chain_verified_events INTEGER
);
INSERT OR IGNORE INTO bus_state(singleton) VALUES(1);
