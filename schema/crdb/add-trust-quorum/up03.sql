CREATE UNIQUE INDEX IF NOT EXISTS lookup_lrtq_members_by_hw_baseboard_id
ON omicron.public.lrtq_members (hw_baseboard_id);
