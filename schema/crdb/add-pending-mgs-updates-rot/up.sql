CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_rot (
    blueprint_id UUID,
    hw_baseboard_id UUID NOT NULL,
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    expected_active_slot omicron.public.hw_rot_slot NOT NULL,
    expected_active_version STRING NOT NULL,
    expected_inactive_version STRING,
    expected_persistent_boot_preference omicron.public.hw_rot_slot NOT NULL,
    expected_pending_persistent_boot_preference omicron.public.hw_rot_slot,
    expected_transient_boot_preference omicron.public.hw_rot_slot,

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);
