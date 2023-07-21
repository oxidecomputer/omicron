BEGIN;
SELECT IF (EXISTS(SELECT value = '1.0.0' OR value = '1.0.1' FROM omicron.public.db_metadata WHERE name = 'schema_version'), TRUE, CAST(1/0 as BOOL));

ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS boot_on_fault BOOL NOT NULL DEFAULT false;

UPDATE omicron.public.db_metadata SET value = '1.0.1' WHERE name = 'schema_version';
COMMIT;

