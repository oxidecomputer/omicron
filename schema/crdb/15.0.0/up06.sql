ALTER TABLE omicron.public.external_ip ADD CONSTRAINT IF NOT EXISTS null_non_fip_parent_id CHECK (
    (kind != 'floating' AND parent_id is NOT NULL) OR (kind = 'floating')
);
