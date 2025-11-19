-- Now, drop the original column that we want to change the type of.
ALTER TABLE omicron.public.ip_pool
DROP COLUMN IF EXISTS reservation_type;
