CREATE TABLE IF NOT EXISTS schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '5')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;