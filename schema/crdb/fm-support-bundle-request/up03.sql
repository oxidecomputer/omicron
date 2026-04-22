CREATE INDEX IF NOT EXISTS
    lookup_fm_support_bundle_request_by_id
ON omicron.public.fm_support_bundle_request (id)
STORING (requested_sitrep_id);
