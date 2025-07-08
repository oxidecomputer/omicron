CREATE INDEX IF NOT EXISTS lookup_export_by_resource_type
ON omicron.public.user_data_export (resource_type);
