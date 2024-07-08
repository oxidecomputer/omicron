CREATE TABLE IF NOT EXISTS omicron.public.timeseries_schema (
    timeseries_name STRING(128) PRIMARY KEY,
    authz_scope omicron.public.timeseries_authz_scope NOT NULL,
    target_description STRING(512) NOT NULL,
    metric_description STRING(512) NOT NULL,
    datum_type omicron.public.timeseries_datum_type NOT NULL,
    units omicron.public.timeseries_units NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    generation INT8 NOT NULL
);
