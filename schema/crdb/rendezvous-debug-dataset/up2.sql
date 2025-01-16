CREATE INDEX IF NOT EXISTS lookup_usable_rendezvous_debug_dataset
    ON omicron.public.rendezvous_debug_dataset (id)
    WHERE time_tombstoned IS NULL;
