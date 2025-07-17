CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_rot_bootloader (
    blueprint_id UUID,
    hw_baseboard_id UUID NOT NULL,
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,
    expected_stage0_version STRING NOT NULL,
    expected_stage0_next_version STRING,

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);
