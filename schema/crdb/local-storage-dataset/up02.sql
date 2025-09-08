CREATE TABLE IF NOT EXISTS omicron.public.local_storage_dataset (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,
    /* FK into the Pool table */
    pool_id UUID NOT NULL,
    /*
     * An upper bound on the amount of space that might be in-use
     *
     * This field is owned by Nexus. When a new row is inserted during the
     * Reconfigurator rendezvous process, this field is set to 0. Reconfigurator
     * otherwise ignores this field. It's updated by Nexus as vmm allocations
     * and deletions are performed using this dataset.
     */
    size_used INT NOT NULL,
    /* Do not consider this dataset during local storage allocation */
    no_provision BOOL NOT NULL
);
