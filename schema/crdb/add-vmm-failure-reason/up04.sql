ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS last_failure_reason omicron.public.vmm_failure_reason;
