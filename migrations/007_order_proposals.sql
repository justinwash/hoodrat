CREATE TABLE IF NOT EXISTS order_proposals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposed_at TEXT NOT NULL,
    run_id TEXT,
    account_number TEXT,
    asset_class TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity REAL,
    notional_usd REAL NOT NULL,
    limit_price REAL,
    quote_age_secs INTEGER,
    source TEXT NOT NULL,
    verdict TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    proposal_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_proposals_proposed_at
    ON order_proposals(proposed_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_proposals_symbol
    ON order_proposals(symbol);

CREATE TABLE IF NOT EXISTS order_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    approved_at TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    proposal_id INTEGER NOT NULL REFERENCES order_proposals(id),
    submitted_task_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_order_approvals_proposal_id
    ON order_approvals(proposal_id);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '8')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;