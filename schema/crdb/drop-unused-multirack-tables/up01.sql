-- We changed the multirack design and decided to track this stuff in the
-- inventory system instead, with new tables added for discovered racks and
-- sleds. This was never used and can be safely dropped.
DROP TABLE IF EXISTS omicron.public.unadopted_sled;
