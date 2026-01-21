CREATE TABLE IF NOT EXISTS omicron.public.inv_health_monitor_svc_in_maintenance (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about);
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,

    -- unique id for each row
    id UUID NOT NULL,

    -- error when calling the svcs command
    svcs_cmd_error TEXT,

    -- time when the status was checked if applicable
    -- TODO-K: This will change to not null with omicron#9615
    time_of_status TIMESTAMPTZ,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);