ALTER TABLE omicron.public.vmm
    ADD COLUMN IF NOT EXISTS failure_reason TEXT;
