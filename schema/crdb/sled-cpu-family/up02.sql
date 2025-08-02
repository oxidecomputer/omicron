ALTER TABLE omicron.public.sled ADD COLUMN IF NOT EXISTS 
    cpu_family omicron.public.sled_cpu_family NOT NULL DEFAULT 'unknown';
