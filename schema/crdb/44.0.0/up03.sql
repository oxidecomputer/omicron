CREATE INDEX IF NOT EXISTS inv_zpool_by_id_and_time ON omicron.public.inv_zpool (id, time_collected DESC);
