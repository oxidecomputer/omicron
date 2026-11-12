CREATE UNIQUE INDEX IF NOT EXISTS lookup_sled_expunge_request_by_rack_and_epoch
    ON omicron.public.sled_expunge_request (rack_id, tq_epoch);
