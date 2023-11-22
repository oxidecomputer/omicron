ALTER TABLE omicron.public.external_ip ADD CONSTRAINT null_project_id IF NOT EXISTS CHECK (
	kind = 'floating' OR project_id IS NULL
);
