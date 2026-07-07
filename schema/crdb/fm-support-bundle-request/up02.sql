CREATE INDEX IF NOT EXISTS
    lookup_fm_support_bundle_requests_for_case
ON omicron.public.fm_support_bundle_request (sitrep_id, case_id);
