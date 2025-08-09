-- backfill host OS ereports part_number column by querying the sled table. if
-- there isn't a sled for the ereport's sled UUID in the sled table (i.e. if the
-- sled record was hard deleted), leave it null.
SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.host_ereport
    SET part_number = sled.part_number
    FROM sled
    WHERE host_ereport.sled_id = sled.id;
