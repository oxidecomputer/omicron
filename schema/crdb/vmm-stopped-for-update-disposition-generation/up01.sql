ALTER TABLE omicron.public.vmm
    ADD COLUMN IF NOT EXISTS stopped_for_update_disposition_generation INT8;
