ALTER TABLE omicron.public.sled_resource_vmm
  ADD COLUMN IF NOT EXISTS instance_state_generation INT
  DEFAULT NULL;
