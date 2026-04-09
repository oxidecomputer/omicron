CREATE UNIQUE INDEX IF NOT EXISTS inv_svc_enabled_not_online_collection_by_sled
    ON omicron.public.inv_svc_enabled_not_online (inv_collection_id, sled_id);