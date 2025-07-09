CREATE INDEX IF NOT EXISTS lookup_export_by_state
ON omicron.public.user_data_export (state);
