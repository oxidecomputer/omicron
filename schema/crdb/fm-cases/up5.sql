-- The same ereport may not be assigned to the same case multiple times.
CREATE UNIQUE INDEX IF NOT EXISTS
    lookup_ereport_assignments_by_ereport
ON omicron.public.fm_ereport_in_case (
    sitrep_id,
    case_id,
    restart_id,
    ena
);
