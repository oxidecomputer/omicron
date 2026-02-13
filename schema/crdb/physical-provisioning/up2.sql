-- Physical provisioning resource table, analogous to
-- virtual_provisioning_resource but tracking actual physical bytes consumed.
--
-- NOTE: This table must be separate from physical_provisioning_collection
-- because CockroachDB does not allow CTEs to modify the same table in
-- multiple subqueries.

CREATE TABLE IF NOT EXISTS omicron.public.physical_provisioning_resource (
    -- Should match the UUID of the corresponding resource.
    id UUID PRIMARY KEY,
    time_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Identifies the type of the resource.
    resource_type STRING(63) NOT NULL,

    -- Physical bytes consumed by writable disk regions (R/W).
    physical_writable_disk_bytes INT8 NOT NULL DEFAULT 0,

    -- Physical bytes consumed by ZFS snapshots (pre-scrub).
    physical_zfs_snapshot_bytes INT8 NOT NULL DEFAULT 0,

    -- Physical bytes consumed by read-only regions (post-scrub or
    -- pre-accounted).
    physical_read_only_disk_bytes INT8 NOT NULL DEFAULT 0,

    -- The number of CPUs provisioned.
    cpus_provisioned INT8 NOT NULL DEFAULT 0,

    -- The amount of RAM provisioned.
    ram_provisioned INT8 NOT NULL DEFAULT 0
);
