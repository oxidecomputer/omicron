ALTER TABLE omicron.public.vmm
    ADD COLUMN IF NOT EXISTS stop_for_update_disposition_generation INT8;
