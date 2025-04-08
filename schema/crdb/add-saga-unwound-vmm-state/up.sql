ALTER TYPE omicron.public.vmm_state
    ADD VALUE IF NOT EXISTS 'saga_unwound' AFTER 'destroyed';
