CREATE INDEX IF NOT EXISTS sled_by_policy
ON omicron.public.sled (sled_policy) STORING (ip, sled_state);
