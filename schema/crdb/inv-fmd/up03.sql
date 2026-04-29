CREATE TABLE IF NOT EXISTS omicron.public.inv_fmd_resource (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    resource_id UUID NOT NULL,
    -- Fault Management Resource Identifier
    -- (e.g. "dev:////pci@af,0/pci1022,1483@3,5").
    fmri TEXT NOT NULL,
    -- The case_id pairs with a corresponding row in inv_fmd_host_case
    -- under the same (inv_collection_id, sled_id) partition.
    case_id UUID NOT NULL,
    faulty BOOL NOT NULL,
    unusable BOOL NOT NULL,
    invisible BOOL NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, resource_id)
);
