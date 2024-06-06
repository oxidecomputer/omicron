CREATE TABLE IF NOT EXISTS omicron.public.migration (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    source_state omicron.public.migration_state NOT NULL,
    source_propolis_id UUID NOT NULL,
    source_gen INT8 NOT NULL DEFAULT 1,
    time_source_updated TIMESTAMPTZ,
    target_state omicron.public.migration_state NOT NULL,
    target_propolis_id UUID NOT NULL,
    target_gen INT8 NOT NULL DEFAULT 1,
    time_target_updated TIMESTAMPTZ
);
