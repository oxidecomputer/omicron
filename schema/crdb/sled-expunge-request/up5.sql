CREATE INDEX IF NOT EXISTS lookup_sled_expunge_request_by_sled
    ON omicron.public.sled_expunge_request (sled_id);
