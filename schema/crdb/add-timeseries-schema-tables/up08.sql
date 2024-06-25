CREATE TABLE IF NOT EXISTS omicron.public.timeseries_field_by_version (
    timeseries_name STRING(128) NOT NULL,
    version INT2 NOT NULL CHECK (version BETWEEN 1 AND 256),
    field_name STRING(128) NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (timeseries_name, version, field_name)
);
