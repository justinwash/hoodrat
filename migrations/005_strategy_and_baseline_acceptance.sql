CREATE TABLE IF NOT EXISTS baseline_acceptances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accepted_at TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    reconciliation_run_id INTEGER NOT NULL REFERENCES reconciliation_runs(id),
    prior_fingerprint_json TEXT NOT NULL,
    accepted_fingerprint_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_baseline_acceptances_accepted_at
    ON baseline_acceptances(accepted_at DESC);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '6')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;