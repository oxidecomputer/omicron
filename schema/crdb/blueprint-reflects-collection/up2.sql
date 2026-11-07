ALTER TABLE omicron.public.blueprint
    ADD CONSTRAINT IF NOT EXISTS reflects_collection_all_or_none CHECK (
        (reflects_collection_id IS NULL) =
            (reflects_collection_time_started IS NULL) AND
        (reflects_collection_id IS NULL) =
            (reflects_collection_time_done IS NULL)
    );
