ALTER TABLE omicron.public.external_ip ADD CONSTRAINT IF NOT EXISTS null_project_id CHECK (
	kind = 'floating' OR project_id IS NULL
);
