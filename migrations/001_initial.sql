CREATE TABLE IF NOT EXISTS agent_runs (
    id TEXT PRIMARY KEY,
    lane TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    prompt TEXT NOT NULL,
    raw_output TEXT,
    summary TEXT
);

CREATE TABLE IF NOT EXISTS agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES agent_runs(id),
    sequence_number INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    text TEXT,
    raw_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES agent_runs(id),
    category TEXT NOT NULL,
    action TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    total_value_usd REAL,
    buying_power_usd REAL,
    realized_pnl_usd REAL,
    unrealized_pnl_usd REAL,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE,
    run_id TEXT REFERENCES agent_runs(id),
    asset_class TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL,
    notional_usd REAL,
    status TEXT NOT NULL,
    submitted_at TEXT,
    filled_at TEXT,
    average_fill_price REAL,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_started_at ON agent_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_events_run_id ON agent_events(run_id, sequence_number);
CREATE INDEX IF NOT EXISTS idx_audit_events_recorded_at ON audit_events(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_submitted_at ON executions(submitted_at DESC);