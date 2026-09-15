ALTER TABLE
omicron.public.external_ip
ADD CONSTRAINT IF NOT EXISTS fips_and_services_need_descriptions CHECK (
    (is_service = TRUE AND description IS NOT NULL) OR
    (is_service = FALSE AND kind != 'floating' AND description IS NULL) OR
    (is_service = FALSE AND kind = 'floating' AND description IS NOT NULL)
);
