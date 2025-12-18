CREATE INDEX IF NOT EXISTS
    lookup_fm_cases_for_sitrep
ON omicron.public.fm_case (sitrep_id);
