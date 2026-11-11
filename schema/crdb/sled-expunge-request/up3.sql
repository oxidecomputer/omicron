CREATE TABLE IF NOT EXISTS omicron.public.sled_expunge_request (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    rack_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    part_number STRING(63) NOT NULL,
    serial_number STRING(63) NOT NULL,

    tq_epoch INT8 NOT NULL,

    state omicron.public.sled_expunge_request_state NOT NULL,
    power_off_outcome omicron.public.sled_expunge_power_off_outcome,
    send_uncertain BOOL NOT NULL,

    last_error TEXT,
    failure_reason TEXT,
    time_completed TIMESTAMPTZ,

    CONSTRAINT tq_epoch_is_positive CHECK (tq_epoch > 0),

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
    )
);
