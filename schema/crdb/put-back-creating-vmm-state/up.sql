ALTER TYPE omicron.public.vmm_state
    ADD VALUE IF NOT EXISTS 'creating' BEFORE 'starting';
