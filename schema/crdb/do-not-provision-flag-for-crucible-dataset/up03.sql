ALTER TABLE omicron.public.zpool
  ADD COLUMN IF NOT EXISTS control_plane_storage_buffer INT NOT NULL DEFAULT 268435456000;
