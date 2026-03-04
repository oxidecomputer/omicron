SET LOCAL disallow_full_table_scans = 'off';
DELETE FROM omicron.public.inv_collection_error WHERE message IS NULL;
ALTER TABLE omicron.public.inv_collection_error ALTER COLUMN message SET NOT NULL;
