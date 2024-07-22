ALTER TABLE omicron.public.dataset ADD CONSTRAINT IF NOT EXISTS ip_and_port_set_for_crucible CHECK (
  (kind != 'crucible') OR
  (kind = 'crucible' AND ip IS NOT NULL and port IS NOT NULL)
);
