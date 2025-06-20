CREATE TABLE IF NOT EXISTS omicron.public.host_ereport (
    restart_id UUID NOT NULL,
    ena INT8 NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- time at which the ereport was collected
    time_collected TIMESTAMPTZ NOT NULL,
    -- UUID of the Nexus instance that collected the ereport
    collector_id UUID NOT NULL,

    -- identity of the reporting sled
    sled_id UUID NOT NULL,
    sled_serial TEXT NOT NULL,

    -- JSON representation of the ereport
    report JSONB NOT NULL,

    PRIMARY KEY (restart_id, ena)
);