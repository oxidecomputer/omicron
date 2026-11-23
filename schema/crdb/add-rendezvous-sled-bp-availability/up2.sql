CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_sled_bp_availability (
    sled_id UUID PRIMARY KEY,
    bp_availability omicron.public.sled_bp_availability NOT NULL,
    update_disposition_generation INT8,
    blueprint_id UUID NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    CONSTRAINT decommissioned_has_no_generation CHECK (
        (bp_availability = 'decommissioned')
        = (update_disposition_generation IS NULL)
    )
);
