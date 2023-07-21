BEGIN;
SELECT IF (EXISTS(SELECT value = '1.0.1' OR value = '1.0.0' FROM omicron.public.db_metadata WHERE name = 'schema_version'), TRUE, CAST(1/0 as BOOL));

ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS boot_on_fault;

UPDATE omicron.public.db_metadata SET value = '1.0.0' WHERE name = 'schema_version';
COMMIT;
