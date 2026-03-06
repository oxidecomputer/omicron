SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.switch_port
    SET switch_slot = 'switch0'::omicron.public.switch_slot
    WHERE switch_location = 'switch0';

UPDATE omicron.public.switch_port
    SET switch_slot = 'switch1'::omicron.public.switch_slot
    WHERE switch_location = 'switch1';

-- This should not remove any rows. Any existing rows where `switch_location`
-- had a value other than 'switch0' or 'switch1' were unusable and would have
-- been causing runtime errors anyway.
DELETE FROM omicron.public.switch_port
    WHERE switch_slot IS NULL;
