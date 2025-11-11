/*
 * Copy existing host ereports into the new ereport table.
 */
set local disallow_full_table_scans = off;

-- host OS ereports
INSERT INTO omicron.public.ereport (
    restart_id,
    ena,
    time_deleted,
    time_collected,
    collector_id,
    serial_number,
    part_number,
    class,
    report,
    reporter,
    sp_type,
    sp_slot,
    sled_id
)
SELECT
    restart_id,
    ena,
    time_deleted,
    time_collected,
    collector_id,
    -- rename this
    sled_serial AS serial_number,
    part_number,
    class,
    report,
    'host'::ereporter_type AS reporter,
    -- sp type and slot should be null for host OS reporters
    NULL AS sp_type,
    NULL AS sp_slot,
    sled_id
FROM
    omicron.public.host_ereport
WHERE
    time_deleted IS NULL;

-- SP ereports
INSERT INTO omicron.public.ereport (
    restart_id,
    ena,
    time_deleted,
    time_collected,
    collector_id,
    serial_number,
    part_number,
    class,
    report,
    reporter,
    sp_type,
    sp_slot,
    sled_id
)
SELECT
    restart_id,
    ena,
    time_deleted,
    time_collected,
    collector_id,
    serial_number,
    part_number,
    class,
    report,
    'sp'::ereporter_type AS reporter,
    sp_type,
    sp_slot,
    -- sled ID shoudl be null for SP reporters
    NULL AS sled_id
FROM
    omicron.public.sp_ereport
WHERE
    time_deleted IS NULL;
