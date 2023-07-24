ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS boot_on_fault;
UPDATE omicron.public.db_metadata SET version = '1.0.0' WHERE singleton = true;
