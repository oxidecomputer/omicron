CREATE INDEX IF NOT EXISTS
    lookup_sitreps_by_parent_id
ON omicron.public.fm_sitrep (parent_sitrep_id);
