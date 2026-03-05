SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bfd_session
    SET switch_loc = 'switch0'::omicron.public.switch_location
    WHERE switch = 'switch0';

UPDATE omicron.public.bfd_session
    SET switch_loc = 'switch1'::omicron.public.switch_location
    WHERE switch = 'switch1';

-- This should not remove any rows. Any existing rows where `switch` has a value
-- other than 'switch0' or 'switch1' is unusable and would have been causing
-- runtime errors anyway.
DELETE FROM omicron.public.bfd_session
    WHERE switch_loc IS NULL;
