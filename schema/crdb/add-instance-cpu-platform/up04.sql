ALTER TABLE omicron.public.vmm 
  ADD COLUMN IF NOT EXISTS cpu_platform omicron.public.vmm_cpu_platform
  DEFAULT 'sled_default';
