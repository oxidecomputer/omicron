CREATE INDEX IF NOT EXISTS
    lookup_fm_alert_requests_for_case
ON omicron.public.fm_alert_request (sitrep_id, case_id);
