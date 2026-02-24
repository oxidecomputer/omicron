-- Physical provisioning collection table, analogous to
-- virtual_provisioning_collection but tracking actual physical bytes consumed.
--
-- Collections exist for:
-- - Projects
-- - Silos
-- - Fleet

CREATE TABLE IF NOT EXISTS omicron.public.physical_provisioning_collection (
    -- Should match the UUID of the corresponding collection.
    id UUID PRIMARY KEY,
    time_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Identifies the type of the collection.
    collection_type STRING(63) NOT NULL,

    -- Physical bytes consumed by writable disk regions (R/W).
    physical_writable_disk_bytes INT8 NOT NULL DEFAULT 0,

    -- Physical bytes consumed by ZFS snapshots (pre-scrub).
    physical_zfs_snapshot_bytes INT8 NOT NULL DEFAULT 0,

    -- Physical bytes consumed by read-only regions (post-scrub or
    -- pre-accounted).
    physical_read_only_disk_bytes INT8 NOT NULL DEFAULT 0,

    -- The number of CPUs provisioned by VMs.
    cpus_provisioned INT8 NOT NULL DEFAULT 0,

    -- The amount of RAM provisioned by VMs.
    ram_provisioned INT8 NOT NULL DEFAULT 0
);
