-- Table of all sled subnets allocated for sleds added to an already initialized
-- rack. The sleds in this table and their allocated subnets are created before
-- a sled is added to the `sled` table. Addition to the `sled` table occurs
-- after the sled is initialized and notifies Nexus about itself.
--
-- For simplicity and space savings, this table doesn't actually contain the
-- full subnets for a given sled, but only the octet that extends a /56 rack
-- subnet to a /64 sled subnet. The rack subnet is maintained in the `rack`
-- table.
--
-- This table does not include subnet octets allocated during RSS and therefore
-- all of the octets start at 33. This makes the data in this table purely additive
-- post-RSS, which also implies that we cannot re-use subnet octets if an original
-- sled that was part of RSS was removed from the cluster.
CREATE TABLE IF NOT EXISTS omicron.public.sled_underlay_subnet_allocation (
    -- The physical identity of the sled
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID PRIMARY KEY,

    -- The rack to which a sled is being added
    -- (foreign key into `rack` table)
    --
    -- We require this because the sled is not yet part of the sled table when
    -- we first allocate a subnet for it.
    rack_id UUID NOT NULL,

    -- The sled to which a subnet is being allocated
    --
    -- Eventually will be a foreign key into the `sled` table when the sled notifies nexus
    -- about itself after initialization.
    sled_id UUID NOT NULL,

    -- The octet that extends a /56 rack subnet to a /64 sled subnet
    --
    -- Always between 33 and 255 inclusive
    subnet_octet INT2 NOT NULL UNIQUE CHECK (subnet_octet BETWEEN 33 AND 255)
);
