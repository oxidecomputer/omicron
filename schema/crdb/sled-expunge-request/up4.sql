CREATE TABLE IF NOT EXISTS omicron.public.sled_expunge_request (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    rack_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    part_number STRING(63) NOT NULL,
    serial_number STRING(63) NOT NULL,
    sp_slot INT4,
    sp_slot_source omicron.public.sled_expunge_sp_slot_source,

    tq_epoch INT8 NOT NULL,

    state omicron.public.sled_expunge_request_state NOT NULL,
    power_off_outcome omicron.public.sled_expunge_power_off_outcome,

    last_error TEXT,
    failure_reason TEXT,
    time_completed TIMESTAMPTZ,

    CONSTRAINT time_completed_iff_finished CHECK (
        (state IN ('done', 'failed') AND time_completed IS NOT NULL)
        OR
        (state NOT IN ('done', 'failed') AND time_completed IS NULL)
    ),

    CONSTRAINT failure_reason_iff_failed CHECK (
        (state = 'failed' AND failure_reason IS NOT NULL)
        OR
        (state != 'failed' AND failure_reason IS NULL)
    ),

    CONSTRAINT power_off_outcome_matches_state CHECK (
        (state IN ('waiting_for_trust_quorum', 'powering_off')
            AND power_off_outcome IS NULL)
        OR
        (state IN ('expunging', 'done') AND power_off_outcome IS NOT NULL)
        OR
        state = 'failed'
    ),

    CONSTRAINT last_error_only_if_unfinished CHECK (
        state IN ('waiting_for_trust_quorum', 'powering_off', 'expunging')
        OR
        last_error IS NULL
    ),

    CONSTRAINT sp_slot_source_iff_sp_slot CHECK (
        (sp_slot IS NULL) = (sp_slot_source IS NULL)
    ),

    CONSTRAINT sp_slot_is_sled_cubby CHECK (
        sp_slot BETWEEN 0 AND 31
    ),

    CONSTRAINT power_off_outcome_matches_sp_slot CHECK (
        power_off_outcome IS NULL
        OR
        ((power_off_outcome = 'absent_at_creation') = (sp_slot IS NULL))
    )
);
