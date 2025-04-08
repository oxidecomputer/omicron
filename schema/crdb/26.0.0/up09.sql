ALTER TABLE IF EXISTS omicron.public.external_ip
ADD CONSTRAINT IF NOT EXISTS null_snat_parent_id CHECK (
    (kind != 'snat') OR (parent_id IS NOT NULL)
);
