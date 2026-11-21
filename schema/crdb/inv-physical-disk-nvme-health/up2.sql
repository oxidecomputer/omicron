ALTER TABLE omicron.public.inv_physical_disk
    ADD COLUMN IF NOT EXISTS nvme_health_time_collected TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS nvme_health_reliability_degraded BOOL,
    ADD COLUMN IF NOT EXISTS nvme_health_read_only BOOL,
    ADD COLUMN IF NOT EXISTS nvme_health_error_kind omicron.public.nvme_health_error_kind,
    ADD COLUMN IF NOT EXISTS nvme_health_error_message TEXT;
