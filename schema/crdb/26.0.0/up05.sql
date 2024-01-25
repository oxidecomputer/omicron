-- Now move the new column to its intended state of non-nullable.
ALTER TABLE omicron.public.external_ip ALTER COLUMN state SET NOT NULL;
