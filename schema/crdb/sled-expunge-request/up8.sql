CREATE INDEX IF NOT EXISTS lookup_sled_expunge_request_by_creation
    ON omicron.public.sled_expunge_request (time_created, id);
