CREATE TABLE IF NOT EXISTS omicron.public.fm_rendezvous_progress (
    singleton BOOL NOT NULL PRIMARY KEY,
    latest_processed_sitrep_version INT8 NOT NULL,

    CHECK (singleton = true)
);
