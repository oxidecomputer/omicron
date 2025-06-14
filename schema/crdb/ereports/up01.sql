CREATE TABLE IF NOT EXISTS omicron.public.sp_ereport (
    restart_id UUID NOT NULL,
    ena INT8 NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- time at which the ereport was collected
    time_collected TIMESTAMPTZ NOT NULL,
    -- UUID of the Nexus instance that collected the ereport
    collector_id UUID NOT NULL,

    -- physical lcoation of the reporting SP
    --
    -- these fields are always present, as they are how requests to collect
    -- ereports are indexed by MGS.
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,

    -- VPD identity of the reporting SP.
    --
    -- unlike the physical location, these fields are nullable, as an ereport
    -- may be generated in a state where the SP doesn't know who or what it is.
    -- consider that "i don't know my own identity" is a reasonable condition to
    -- might want to generate an ereport about!
    serial_number STRING,
    part_number STRING,

    -- JSON representation of the ereport
    report JSONB NOT NULL,

    PRIMARY KEY (restart_id, ena)
);