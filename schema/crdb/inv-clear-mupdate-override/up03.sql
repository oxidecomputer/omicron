ALTER TABLE omicron.public.inv_sled_config_reconciler
    ADD CONSTRAINT IF NOT EXISTS clear_mupdate_override_consistency CHECK (
        (clear_mupdate_override_boot_success IS NULL
         AND clear_mupdate_override_boot_error IS NULL
         AND clear_mupdate_override_non_boot_message IS NULL)
    OR
        (clear_mupdate_override_boot_success IS NOT NULL
         AND clear_mupdate_override_boot_error IS NULL
         AND clear_mupdate_override_non_boot_message IS NOT NULL)
    OR
        (clear_mupdate_override_boot_success IS NULL
         AND clear_mupdate_override_boot_error IS NOT NULL
         AND clear_mupdate_override_non_boot_message IS NOT NULL)
    );
