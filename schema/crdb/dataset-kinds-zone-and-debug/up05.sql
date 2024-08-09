ALTER TABLE omicron.public.dataset ADD CONSTRAINT IF NOT EXISTS zone_name_for_zone_kind CHECK (
      (kind != 'zone') OR
      (kind = 'zone' AND zone_name IS NOT NULL)
)
