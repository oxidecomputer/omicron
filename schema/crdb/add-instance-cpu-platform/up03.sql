ALTER TABLE omicron.public.instance 
  ADD COLUMN IF NOT EXISTS cpu_platform omicron.public.instance_cpu_platform;
