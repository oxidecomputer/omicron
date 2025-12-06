-- Ensure that each sled always has a `hw_baseboard_id`.
--
-- It would be weird if this wasn't true, but we want to guarantee it before
-- upgrade from LRTQ to TQ.
INSERT INTO omicron.public.hw_baseboard_id
     (id, part_number, serial_number)
 SELECT
     gen_random_uuid(), part_number, serial_number
 FROM omicron.public.sled as sled
 ON CONFLICT DO NOTHING;


-- Put all `hw_baseboard_id`s for non-expunged sleds into `lrtq_members`
INSERT INTO omicron.public.lrtq_members
     (rack_id, hw_baseboard_id)
 SELECT
     sled.rack_id, hw.id
 FROM omicron.public.sled as sled
 INNER JOIN omicron.public.hw_baseboard_id as hw
 ON
   sled.part_number = hw.part_number
   AND sled.serial_number = hw.serial_number
   AND sled.sled_policy != 'expunged'
ON CONFLICT DO NOTHING;
