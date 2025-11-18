-- And now drop the temporary column
ALTER TABLE omicron.public.ip_pool
DROP COLUMN IF EXISTS reservation_type_temp;
