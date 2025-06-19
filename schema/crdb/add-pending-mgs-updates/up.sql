CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_sp (
    blueprint_id UUID,
    hw_baseboard_id UUID NOT NULL,
    sp_slot INT4 NOT NULL,
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    expected_active_version STRING NOT NULL,
    expected_inactive_version STRING,

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);
