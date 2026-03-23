CREATE UNIQUE INDEX IF NOT EXISTS
   lookup_sitrep_version_by_id
ON omicron.public.fm_sitrep_history (sitrep_id);
