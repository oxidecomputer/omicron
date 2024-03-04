CREATE UNIQUE INDEX IF NOT EXISTS background_task_toggles_name
ON omicron.public.background_task_toggles (name)
STORING (enabled);
