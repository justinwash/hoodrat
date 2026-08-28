CREATE TABLE IF NOT EXISTS paper_simulations (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    profile TEXT NOT NULL,
    status TEXT NOT NULL,
    starting_cash_usd REAL NOT NULL,
    final_cash_usd REAL NOT NULL,
    final_equity_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL,
    unrealized_pnl_usd REAL NOT NULL,
    market_snapshot_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_simulations_finished_at
    ON paper_simulations(finished_at DESC);

CREATE TABLE IF NOT EXISTS paper_simulation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    simulation_id TEXT NOT NULL REFERENCES paper_simulations(id),
    event_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    notional_usd REAL NOT NULL,
    fee_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL,
    details_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_simulation_events_simulation_id
    ON paper_simulation_events(simulation_id, event_at);

CREATE TABLE IF NOT EXISTS paper_simulation_positions (
    simulation_id TEXT NOT NULL REFERENCES paper_simulations(id),
    position_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    quantity REAL NOT NULL,
    average_entry_price REAL NOT NULL,
    mark_price REAL NOT NULL,
    market_value_usd REAL NOT NULL,
    unrealized_pnl_usd REAL NOT NULL,
    opened_at TEXT NOT NULL,
    underlying TEXT,
    option_type TEXT,
    strike REAL,
    expiration TEXT,
    multiplier REAL NOT NULL,
    PRIMARY KEY (simulation_id, position_key)
);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '7')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;