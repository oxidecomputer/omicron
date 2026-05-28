-- Strip trailing NUL (\0) characters from the serial_number column in the
-- ereport table. The service processor null-pads OXV2 serial numbers. MGS
-- was stripping these trailing NULLs when collecting the inventory, but
-- not when ingesting ereports. This was fixed in
-- https://github.com/oxidecomputer/management-gateway-service#491
-- so we can now update any previously-nully ereport serials to match.
SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.ereport
    SET serial_number = rtrim(serial_number, chr(0))
    WHERE serial_number IS NOT NULL
    AND serial_number != rtrim(serial_number, chr(0));
