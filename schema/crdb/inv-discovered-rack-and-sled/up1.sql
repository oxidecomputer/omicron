-- Inventory of other racks discovered via DDM that are not necessarily part of
-- the multirack cluster yet.
CREATE TABLE IF NOT EXISTS omicron.public.inv_discovered_rack (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- This is the same id that gets used in the `rack` table if/when the rack
    -- is adopted:
    rack_id UUID NOT NULL,

    -- The IPv6 underlay /56 prefix for the rack:
    rack_subnet INET NOT NULL,

    -- Underlay address of the sled running the multirack join service:
    join_service_ip INET NOT NULL,

    -- Only allow IPv6 INET values for rack_subnet:
    CONSTRAINT rack_subnet_family CHECK (
        family(rack_subnet) = 6
    ),
    -- Enforce the correct length of the rack subnet:
    CONSTRAINT rack_subnet_prefix CHECK (
        masklen(rack_subnet) = 56
    ),

    PRIMARY KEY (inv_collection_id, rack_id)
);
