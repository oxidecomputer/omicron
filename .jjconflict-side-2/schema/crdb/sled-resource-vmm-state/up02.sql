ALTER TABLE omicron.public.sled_resource_vmm
  ADD COLUMN IF NOT EXISTS
   state omicron.public.sled_resource_vmm_state NOT NULL
  DEFAULT 'active';
