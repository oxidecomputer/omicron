SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bfd_session
    SET switch_slot = 'switch0'::omicron.public.switch_slot
    WHERE switch = 'switch0';

UPDATE omicron.public.bfd_session
    SET switch_slot = 'switch1'::omicron.public.switch_slot
    WHERE switch = 'switch1';

-- This should not remove any rows. Any existing rows where `switch` had a value
-- other than 'switch0' or 'switch1' were unusable and would have been causing
-- runtime errors anyway.
DELETE FROM omicron.public.bfd_session
    WHERE switch_slot IS NULL;
