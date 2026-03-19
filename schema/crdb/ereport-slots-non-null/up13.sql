ALTER TABLE omicron.public.ereport
ADD CONSTRAINT IF NOT EXISTS
    reporter_identity_validity
CHECK (
    (
        -- ereports from SPs must not have a sled ID.
        reporter = 'sp'
            AND sled_id IS NULL
    ) OR (
        -- ereports from the sled host OS must have slot_type = 'sled'
        -- and a sled ID.
        reporter = 'host'
            AND slot_type = 'sled'
            AND sled_id IS NOT NULL
    )
);
