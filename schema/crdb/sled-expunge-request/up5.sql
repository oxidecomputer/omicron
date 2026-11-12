CREATE UNIQUE INDEX IF NOT EXISTS lookup_unfinished_sled_expunge_request_by_sled
    ON omicron.public.sled_expunge_request (sled_id)
    WHERE state IN ('waiting_for_trust_quorum', 'powering_off', 'expunging');
