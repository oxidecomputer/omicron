CREATE TABLE IF NOT EXISTS omicron.public.inv_health_monitor_svc_in_maintenance_error (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about);
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,

    -- unique id for each row
    id UUID NOT NULL,

    -- an error message found when retrieving the SMF services in maintenance
    error_message TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);