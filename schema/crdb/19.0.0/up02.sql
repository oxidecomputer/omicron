ALTER TABLE omicron.public.external_ip ADD CONSTRAINT IF NOT EXISTS null_project_id CHECK (
    (kind = 'floating' AND is_service = FALSE AND project_id IS NOT NULL) OR
    ((kind != 'floating' OR is_service = TRUE) AND project_id IS NULL)
);
