ALTER TABLE omicron.public.bp_omicron_physical_disk
ADD CONSTRAINT IF NOT EXISTS expunged_disposition_properties CHECK (
    (disposition != 'expunged'
        AND disposition_expunged_as_of_generation IS NULL
        AND NOT disposition_expunged_ready_for_cleanup)
    OR
    (disposition = 'expunged'
        AND disposition_expunged_as_of_generation IS NOT NULL)
);
