ALTER TABLE omicron.public.inv_sled_agent ADD COLUMN IF NOT EXISTS 
    cpu_family omicron.public.sled_cpu_family NOT NULL DEFAULT 'unknown';
