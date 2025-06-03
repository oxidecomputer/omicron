-- Prevent the physical_disk table from seeing anything but
-- U.2s until we decide to increase scope.
ALTER TABLE omicron.public.physical_disk
ADD CONSTRAINT IF NOT EXISTS physical_disk_variant_u2 CHECK (variant = 'u2');
