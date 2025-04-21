ALTER TABLE omicron.public.instance 
  ADD COLUMN IF NOT EXISTS min_cpu_platform omicron.public.instance_min_cpu_platform;
