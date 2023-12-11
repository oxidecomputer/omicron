/*
 * Recreate the metric producer assignment table.
 *
 * Note that we're adding the `kind` column here, using the new enum in the
 * previous update SQL file.
 */
CREATE TABLE IF NOT EXISTS omicron.public.metric_producer (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    kind omicron.public.producer_kind,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,
    interval FLOAT NOT NULL,
    base_route STRING(512) NOT NULL,
    oximeter_id UUID NOT NULL
);
