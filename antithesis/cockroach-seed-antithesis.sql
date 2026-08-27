-- TODO-RAINCLAUDE: applied by cockroach-seed.sh after dbinit.sql. Rows here stand in for state that real deployments get from services the phase 1 Antithesis topology does not run.

-- TODO-RAINCLAUDE: the RSS handoff sets a port-settings id on the uplink port named in the rack network config (nexus/src/app/rack.rs, `switch_port_get_id`), and that row is normally created by Nexus's populate_switch_ports task from dendrite's port list. There is no dendrite here, so the placeholder uplink port that sled-agent-sim reports is created up front. The rack id is the one hard-coded in sled-agent/src/sim/server.rs `handoff_to_nexus` and in antithesis/config/nexus.toml.
INSERT INTO omicron.public.switch_port (id, rack_id, port_name, port_settings_id, switch_slot)
VALUES (gen_random_uuid(), 'c19a698f-c6f9-4a17-ae30-20d711b8f7dc', 'qsfp0', NULL, 'switch0');
