SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.inv_collection_error SET message = '' WHERE message IS NULL;
ALTER TABLE omicron.public.inv_collection_error ALTER COLUMN message SET NOT NULL;
