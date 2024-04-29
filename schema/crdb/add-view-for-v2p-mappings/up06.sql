CREATE INDEX IF NOT EXISTS sled_by_policy
ON sled (sled_policy) STORING (ip, sled_state);
