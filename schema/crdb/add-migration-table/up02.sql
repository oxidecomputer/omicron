CREATE TABLE IF NOT EXISTS omicron.public.migration (
    id UUID PRIMARY KEY,
    source_state omicron.public.migration_state NOT NULL,
    source_propolis_id UUID NOT NULL,
    source_gen INT8 NOT NULL DEFAULT 1,
    target_state omicron.public.migration_state NOT NULL,
    target_propolis_id UUID NOT NULL,
    target_gen INT8 NOT NULL DEFAULT 1
);
