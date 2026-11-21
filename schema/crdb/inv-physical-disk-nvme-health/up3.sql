ALTER TABLE omicron.public.inv_physical_disk
    ADD CONSTRAINT IF NOT EXISTS nvme_health_reading_or_error CHECK (
        (nvme_health_time_collected IS NULL)
            = (nvme_health_reliability_degraded IS NULL)
        AND (nvme_health_time_collected IS NULL)
            = (nvme_health_read_only IS NULL)
        AND (nvme_health_error_kind IS NULL)
            = (nvme_health_error_message IS NULL)
        AND (
            nvme_health_time_collected IS NULL
            OR nvme_health_error_kind IS NULL
        )
    );
