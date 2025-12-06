CREATE INDEX IF NOT EXISTS lookup_lrtq_members_by_rack_id
ON omicron.public.lrtq_members (rack_id);
