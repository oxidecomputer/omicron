CREATE INDEX IF NOT EXISTS sled_by_policy_and_state
ON omicron.public.sled (sled_policy, sled_state, id) STORING (ip);
