CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_host_phase_1 (
    blueprint_id UUID,
    hw_baseboard_id UUID NOT NULL,
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,
    expected_active_phase_1_slot omicron.public.hw_m2_slot NOT NULL,
    expected_boot_disk omicron.public.hw_m2_slot NOT NULL,
    expected_active_phase_1_hash STRING(64) NOT NULL,
    expected_active_phase_2_hash STRING(64) NOT NULL,
    expected_inactive_phase_1_hash STRING(64) NOT NULL,
    expected_inactive_phase_2_hash STRING(64) NOT NULL,
    sled_agent_ip INET NOT NULL,
    sled_agent_port INT4 NOT NULL CHECK (sled_agent_port BETWEEN 0 AND 65535),
    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);
