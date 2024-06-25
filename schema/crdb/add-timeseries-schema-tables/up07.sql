CREATE TABLE IF NOT EXISTS omicron.public.timeseries_field (
    timeseries_name STRING(128) NOT NULL,
    name STRING(128) NOT NULL,
    source omicron.public.timeseries_field_source NOT NULL,
    type_ omicron.public.timeseries_field_type NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (timeseries_name, name)
);
