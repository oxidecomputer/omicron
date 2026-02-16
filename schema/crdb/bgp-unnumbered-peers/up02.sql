-- Drop the existing primary key constraint and add the new primary key using
-- the id column. These must be in the same transaction because CockroachDB
-- doesn't allow dropping a primary key without immediately adding a new one.
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
    DROP CONSTRAINT IF EXISTS switch_port_settings_bgp_peer_config_pkey,
    ADD CONSTRAINT switch_port_settings_bgp_peer_config_pkey PRIMARY KEY (id);
