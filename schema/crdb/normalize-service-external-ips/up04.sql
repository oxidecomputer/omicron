ALTER TABLE
omicron.public.external_ip
ADD CONSTRAINT IF NOT EXISTS fips_and_services_need_names CHECK (
    (is_service = TRUE AND name IS NOT NULL) OR
    (is_service = FALSE AND kind != 'floating' AND name IS NULL) OR
    (is_service = FALSE AND kind = 'floating' AND name IS NOT NULL)
);
