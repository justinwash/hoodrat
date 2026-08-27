CREATE TABLE IF NOT EXISTS agent_tool_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES agent_runs(id),
    sequence_number INTEGER NOT NULL,
    tool_name TEXT NOT NULL,
    input_json TEXT,
    output_json TEXT,
    is_error INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL,
    UNIQUE (run_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_agent_tool_events_run_id
    ON agent_tool_events(run_id, sequence_number);