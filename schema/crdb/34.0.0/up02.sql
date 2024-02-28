CREATE UNIQUE INDEX IF NOT EXISTS background_task_toggles_name
ON background_task_toggles (name)
STORING (enabled)
