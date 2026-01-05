CREATE INDEX IF NOT EXISTS
    lookup_ereports_assigned_to_fm_case
ON omicron.public.fm_ereport_in_case (sitrep_id, case_id);
