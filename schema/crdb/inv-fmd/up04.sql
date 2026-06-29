CREATE TABLE IF NOT EXISTS omicron.public.inv_fmd_resource (
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,
    resource_id UUID NOT NULL,
    -- Fault Management Resource Identifier
    -- (e.g. "dev:////pci@af,0/pci1022,1483@3,5").
    fmri TEXT NOT NULL,
    -- (foreign key into `inv_fmd_host_case`, with the same
    -- (inv_collection_id, sled_id))
    case_id UUID NOT NULL,
    faulty BOOL NOT NULL,
    unusable BOOL NOT NULL,
    invisible BOOL NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, resource_id)
);
