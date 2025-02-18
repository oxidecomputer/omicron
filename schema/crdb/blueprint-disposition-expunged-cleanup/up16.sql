ALTER TABLE omicron.public.bp_omicron_zone
ADD CONSTRAINT IF NOT EXISTS expunged_disposition_properties CHECK (
    (disposition != 'expunged'
        AND expunged_as_of_generation IS NULL
        AND NOT expunged_confirmed_shut_down)
    OR
    (disposition = 'expunged'
        AND expunged_as_of_generation IS NOT NULL)
);
