ALTER TABLE omicron.public.inv_sled_config_reconciler
    ADD COLUMN IF NOT EXISTS clear_mupdate_override_boot_success omicron.public.clear_mupdate_override_boot_success,
    ADD COLUMN IF NOT EXISTS clear_mupdate_override_boot_error TEXT,
    ADD COLUMN IF NOT EXISTS clear_mupdate_override_non_boot_message TEXT;
