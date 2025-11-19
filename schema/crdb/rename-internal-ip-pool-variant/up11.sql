-- And finally, drop the default we added to simplify the data migration
ALTER TABLE omicron.public.ip_pool
ALTER COLUMN reservation_type DROP DEFAULT;
