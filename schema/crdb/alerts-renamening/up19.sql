-- We are about to create a new table and drop this one, as we cannot easily
-- change the enum column types otherwise... :(
ALTER TABLE IF EXISTS omicron.public.webhook_delivery
RENAME TO omicron.public.webhook_delivery_old;
