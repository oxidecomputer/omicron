ALTER TABLE omicron.public.fm_config ADD CONSTRAINT IF NOT EXISTS certificate_expiry_warning_days_validity CHECK (
    certificate_expiry_warning_days IS NULL OR (
        certificate_expiry_warning_days >= 1 AND
        certificate_expiry_warning_days <= 3650
    )
);
