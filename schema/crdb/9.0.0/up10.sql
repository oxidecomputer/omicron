CREATE INDEX IF NOT EXISTS errors_by_collection
    ON omicron.public.inv_collection_error (inv_collection_id, idx);
