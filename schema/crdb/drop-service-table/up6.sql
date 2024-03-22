ALTER TABLE omicron.public.sled_resource
    ADD COLUMN IF NOT EXISTS kind omicron.public.sled_resource_kind;
