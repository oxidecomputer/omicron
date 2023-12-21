ALTER TABLE omicron.public.external_ip
ADD CONSTRAINT detached_null_parent_id CHECK (
    (state = 'detached') OR (parent_id IS NOT NULL)
);
