CREATE INDEX IF NOT EXISTS inv_collectionby_time_done 
    ON omicron.public.inv_collection (time_done DESC);
