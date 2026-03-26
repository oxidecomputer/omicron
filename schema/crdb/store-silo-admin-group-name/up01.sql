ALTER TABLE omicron.public.silo
ADD COLUMN IF NOT EXISTS
admin_group_name TEXT DEFAULT NULL
