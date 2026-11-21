ALTER TABLE omicron.public.fm_fact_physical_disk
    ADD COLUMN IF NOT EXISTS nvme_reliability_degraded BOOL,
    ADD COLUMN IF NOT EXISTS nvme_read_only BOOL;
