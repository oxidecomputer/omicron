ALTER TABLE omicron.public.external_ip
ADD CONSTRAINT IF NOT EXISTS detached_null_parent_id CHECK (
    (state = 'detached') OR (parent_id IS NOT NULL)
);
