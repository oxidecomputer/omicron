CREATE INDEX IF NOT EXISTS inv_collection_by_time_started
    ON omicron.public.inv_collection (time_started);
