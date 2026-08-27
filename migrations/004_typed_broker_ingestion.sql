CREATE TABLE IF NOT EXISTS broker_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    account_number TEXT NOT NULL,
    rhs_account_number TEXT,
    rhc_account_number TEXT,
    account_type TEXT,
    brokerage_account_type TEXT,
    nickname TEXT,
    is_default INTEGER,
    agentic_allowed INTEGER,
    option_level TEXT,
    management_type TEXT,
    affiliate TEXT,
    state TEXT,
    deactivated INTEGER,
    permanently_deactivated INTEGER,
    raw_json TEXT NOT NULL,
    UNIQUE (captured_at, account_number)
);

CREATE INDEX IF NOT EXISTS idx_broker_accounts_captured_at
    ON broker_accounts(captured_at DESC);

CREATE TABLE IF NOT EXISTS broker_balances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    account_number TEXT,
    cash_usd REAL,
    buying_power_usd REAL,
    unleveraged_buying_power_usd REAL,
    equity_usd REAL,
    margin_used_usd REAL,
    unsettled_funds_usd REAL,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_balances_captured_at
    ON broker_balances(captured_at DESC);

CREATE TABLE IF NOT EXISTS broker_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    account_number TEXT,
    symbol TEXT,
    instrument_id TEXT,
    asset_class TEXT,
    quantity REAL,
    average_cost_usd REAL,
    market_value_usd REAL,
    current_price_usd REAL,
    unrealized_pnl_usd REAL,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_positions_captured_at
    ON broker_positions(captured_at DESC);

CREATE TABLE IF NOT EXISTS broker_pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    account_number TEXT,
    span TEXT,
    start_date TEXT,
    end_date TEXT,
    realized_pnl_usd REAL,
    total_returns_usd REAL,
    rate_of_realized_gain REAL,
    total_rate_of_return REAL,
    number_of_trades INTEGER,
    by_asset_class_json TEXT,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_pnl_snapshots_captured_at
    ON broker_pnl_snapshots(captured_at DESC);

CREATE TABLE IF NOT EXISTS broker_pnl_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    account_number TEXT,
    external_id TEXT,
    symbol TEXT,
    asset_class TEXT,
    side TEXT,
    quantity REAL,
    realized_pnl_usd REAL,
    opened_at TEXT,
    closed_at TEXT,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_pnl_trades_captured_at
    ON broker_pnl_trades(captured_at DESC);

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    status TEXT NOT NULL,
    account_count INTEGER NOT NULL,
    balance_count INTEGER NOT NULL,
    position_count INTEGER NOT NULL,
    pnl_trade_count INTEGER NOT NULL,
    drift_json TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    fingerprint_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_captured_at
    ON reconciliation_runs(captured_at DESC);