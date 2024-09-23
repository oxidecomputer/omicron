ALTER TABLE omicron.public.vmm
    ADD COLUMN IF NOT EXISTS failure_reason omicron.public.vmm_failure_reason;
