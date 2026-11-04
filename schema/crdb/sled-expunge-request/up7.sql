CREATE UNIQUE INDEX IF NOT EXISTS lookup_unfinished_sled_expunge_request_by_baseboard
    ON omicron.public.sled_expunge_request (part_number, serial_number)
    WHERE state IN ('waiting_for_trust_quorum', 'powering_off', 'expunging');
