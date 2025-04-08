/* Add an index which lets us look up sleds based on policy and state */
CREATE INDEX IF NOT EXISTS lookup_sled_by_policy_and_state ON omicron.public.sled (
    sled_policy,
    sled_state
);
