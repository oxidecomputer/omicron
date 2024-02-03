CREATE UNIQUE INDEX IF NOT EXISTS one_ephemeral_ip_per_instance ON omicron.public.external_ip (
    parent_id
)
    WHERE kind = 'ephemeral' AND parent_id IS NOT NULL AND time_deleted IS NULL;
