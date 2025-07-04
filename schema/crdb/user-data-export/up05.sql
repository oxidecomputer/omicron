CREATE UNIQUE INDEX IF NOT EXISTS one_export_record_per_resource
ON omicron.public.user_data_export (resource_id)
WHERE state != 'deleted';
