ALTER TABLE omicron.public.fm_fact_physical_disk
    ADD CONSTRAINT IF NOT EXISTS nvme_health_warning_columns_present CHECK (
        kind != 'nvme_health_warning' OR (
            nvme_reliability_degraded IS NOT NULL
            AND nvme_read_only IS NOT NULL
            AND observed_in_inv IS NOT NULL
            AND time_observed IS NOT NULL
        )
    );
