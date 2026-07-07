ALTER TABLE omicron.public.ereport
ADD CONSTRAINT IF NOT EXISTS reporter_identity_validity
CHECK (
    (
        -- ereports from SPs must have a slot type and slot number, and
        -- must not have a sled ID.
        reporter = 'sp'
            AND slot IS NOT NULL
            AND sled_id IS NULL
    ) OR (
        -- ereports from the sled host OS must have a sled ID, and must
        -- have the 'sled' slot type (as switches and PSCs do not have
        -- a host OS)
        reporter = 'host'
            AND sled_id IS NOT NULL
            AND slot_type = 'sled'
    )
);
