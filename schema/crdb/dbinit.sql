/*
 * dbinit.sql: raw SQL to initialize a database for use by Omicron
 *
 * It's not clear what the long-term story for managing the database schema will
 * be.  For now, this file can be used by the test suite and by developers (via
 * the "omicron-dev" program) to set up a local database with which to run the
 * system.
 */

/*
 * Important CockroachDB notes:
 *
 *    For timestamps, CockroachDB's docs recommend TIMESTAMPTZ rather than
 *    TIMESTAMP.  This does not change what is stored with each datum, but
 *    rather how it's interpreted when clients use it.  It should make no
 *    difference to us, so we stick with the recommendation.
 *
 *    We avoid explicit foreign keys due to this warning from the docs: "Foreign
 *    key dependencies can significantly impact query performance, as queries
 *    involving tables with foreign keys, or tables referenced by foreign keys,
 *    require CockroachDB to check two separate tables. We recommend using them
 *    sparingly."
 */

BEGIN;

/*
 * We assume the database and user do not already exist so that we don't
 * inadvertently clobber what's there.  If they might exist, the user has to
 * clear this first.
 *
 * NOTE: the database and user names MUST be kept in sync with the
 * initialization code and dbwipe.sql.
 */
CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;
ALTER DEFAULT PRIVILEGES GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES to omicron;

/*
 * Configure a replication factor of 5 to ensure that the system can maintain
 * availability in the face of any two node failures.
 */
ALTER RANGE default CONFIGURE ZONE USING num_replicas = 5;


/*
 * The deployment strategy for clickhouse
 */
CREATE TYPE IF NOT EXISTS omicron.public.clickhouse_mode AS ENUM (
   -- Only deploy a single node clickhouse
   'single_node_only',

   -- Only deploy a clickhouse cluster without any single node deployments
   'cluster_only',

   -- Deploy both a single node and cluster deployment.
   -- This is the strategy for stage 1 described in RFD 468
   'both'
);

/*
 * A planning policy for clickhouse for a single multirack setup
 *
 * We currently implicitly tie this policy to a rack, as we don't yet support
 * multirack. Multiple parts of this database schema are going to have to change
 * to support multirack, so we add one more for now.
 */
CREATE TABLE IF NOT EXISTS omicron.public.clickhouse_policy (
    -- Monotonically increasing version for all policies
    --
    -- This is similar to `bp_target` which will also require being changed for
    -- multirack to associate with some sort of rack group ID.
    version INT8 PRIMARY KEY,

    clickhouse_mode omicron.public.clickhouse_mode NOT NULL,

    -- Only greater than 0 when clickhouse cluster is enabled
    clickhouse_cluster_target_servers INT2 NOT NULL,
    -- Only greater than 0 when clickhouse cluster is enabled
    clickhouse_cluster_target_keepers INT2 NOT NULL,

    time_created TIMESTAMPTZ NOT NULL
);


/*
 * The ClickHouse installation Oximeter should read from
 */
CREATE TYPE IF NOT EXISTS omicron.public.oximeter_read_mode AS ENUM (
   -- Read from the single node ClickHouse installation
   'single_node',

   -- Read from the replicated ClickHouse cluster
   'cluster'
);

/*
 * A planning policy for oximeter_read for a single multirack setup
 */
CREATE TABLE IF NOT EXISTS omicron.public.oximeter_read_policy (
    -- Monotonically increasing version for all policies
    version INT8 PRIMARY KEY,

    oximeter_read_mode omicron.public.oximeter_read_mode NOT NULL,

    time_created TIMESTAMPTZ NOT NULL
);

/*
* Oximeter read policy defaults to reading from a single node ClickHouse server.
*/
INSERT INTO omicron.public.oximeter_read_policy (
    version,
    oximeter_read_mode,
    time_created
) VALUES (
    1,
    'single_node',
    NOW()
) ON CONFLICT DO NOTHING;

/*
 * Racks
 */
CREATE TABLE IF NOT EXISTS omicron.public.rack (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /*
     * Identifies if rack management has been transferred from RSS -> Nexus.
     * If "false", RSS is still managing sleds, services, and DNS records.
     *
     * This value is set to "true" when RSS calls the
     * "rack_initialization_complete" endpoint on Nexus' internal interface.
     *
     * See RFD 278 for more detail.
     */
    initialized BOOL NOT NULL,

    /* Used to configure the updates service URL */
    tuf_base_url STRING(512),

    /* The IPv6 underlay /56 prefix for the rack */
    rack_subnet INET
);

/*
 * Sleds
 */

-- The disposition for a particular sled. This is updated solely by the
-- operator, and not by Nexus.
CREATE TYPE IF NOT EXISTS omicron.public.sled_policy AS ENUM (
    -- The sled is in service, and new resources can be provisioned onto it.
    'in_service',
    -- The sled is in service, but the operator has indicated that new
    -- resources should not be provisioned onto it.
    'no_provision',
    -- The operator has marked that the sled has, or will be, removed from the
    -- rack, and it should be assumed that any resources currently on it are
    -- now permanently missing.
    'expunged'
);

-- The actual state of the sled. This is updated exclusively by Nexus.
--
-- Nexus's goal is to match the sled's state with the operator-indicated
-- policy. For example, if the sled_policy is "expunged" and the sled_state is
-- "active", Nexus will assume that the sled is gone. Based on that, Nexus will
-- reallocate resources currently on the expunged sled to other sleds, etc.
-- Once the expunged sled no longer has any resources attached to it, Nexus
-- will mark it as decommissioned.
CREATE TYPE IF NOT EXISTS omicron.public.sled_state AS ENUM (
    -- The sled has resources of any kind allocated on it, or, is available for
    -- new resources.
    --
    -- The sled can be in this state and have a different sled policy, e.g.
    -- "expunged".
    'active',

    -- The sled no longer has resources allocated on it, now or in the future.
    --
    -- This is a terminal state. This state is only valid if the sled policy is
    -- 'expunged'.
    'decommissioned'
);

-- The model of CPU installed in a particular sled, discovered by sled-agent
-- and reported to Nexus. This determines what VMs can run on a sled: instances
-- that require a specific CPU platform can only run on sleds whose CPUs support
-- all the features of that platform.
CREATE TYPE IF NOT EXISTS omicron.public.sled_cpu_family AS ENUM (
    -- Sled-agent didn't recognize the sled's CPU.
    'unknown',

    -- AMD Milan, or lab CPU close enough that sled-agent reported it as one.
    'amd_milan',

    -- AMD Turin, or lab CPU close enough that sled-agent reported it as one.
    'amd_turin',

    -- AMD Turin Dense. There are no "Turin Dense-likes", so this is precise.
    'amd_turin_dense'
);

CREATE TABLE IF NOT EXISTS omicron.public.sled (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Rack table */
    rack_id UUID NOT NULL,

    /* Idenfities if this Sled is a Scrimlet */
    is_scrimlet BOOL NOT NULL,

    /* Baseboard information about the sled */
    serial_number STRING(63) NOT NULL,
    part_number STRING(63) NOT NULL,
    revision INT8 NOT NULL,

    /* CPU & RAM summary for the sled */
    usable_hardware_threads INT8 CHECK (usable_hardware_threads BETWEEN 0 AND 4294967295) NOT NULL,
    usable_physical_ram INT8 NOT NULL,
    reservoir_size INT8 CHECK (reservoir_size < usable_physical_ram) NOT NULL,

    /* The IP address and bound port of the sled agent server. */
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    /* The last address allocated to a propolis instance on this sled. */
    last_used_address INET NOT NULL,

    /* The policy for the sled, updated exclusively by the operator */
    sled_policy omicron.public.sled_policy NOT NULL,

    /* The actual state of the sled, updated exclusively by Nexus */
    sled_state omicron.public.sled_state NOT NULL,

    /* Generation number owned and incremented by the sled-agent */
    sled_agent_gen INT8 NOT NULL DEFAULT 1,

    /* The bound port of the Repo Depot API server, running on the same IP as
       the sled agent server. */
    repo_depot_port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    /* The sled's detected CPU family. */
    cpu_family omicron.public.sled_cpu_family NOT NULL
);

-- Add an index that ensures a given physical sled (identified by serial and
-- part number) can only be a commissioned member of the control plane once.
--
-- TODO Should `sled` reference `hw_baseboard_id` instead of having its own
-- serial/part columns?
CREATE UNIQUE INDEX IF NOT EXISTS commissioned_sled_uniqueness
    ON omicron.public.sled (serial_number, part_number)
    WHERE sled_state != 'decommissioned';

/* Add an index which lets us look up sleds on a rack */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_sled_by_rack ON omicron.public.sled (
    rack_id,
    id
) WHERE time_deleted IS NULL;

/* Add an index which lets us look up sleds based on policy and state */
CREATE INDEX IF NOT EXISTS lookup_sled_by_policy_and_state ON omicron.public.sled (
    sled_policy,
    sled_state
);

-- Accounting for VMMs using resources on a sled
CREATE TABLE IF NOT EXISTS omicron.public.sled_resource_vmm (
    -- Should match the UUID of the corresponding VMM
    id UUID PRIMARY KEY,

    -- The sled where resources are being consumed
    sled_id UUID NOT NULL,

    -- The maximum number of hardware threads usable by this VMM
    hardware_threads INT8 NOT NULL,

    -- The maximum amount of RSS RAM provisioned to this VMM
    rss_ram INT8 NOT NULL,

    -- The maximum amount of Reservoir RAM provisioned to this VMM
    reservoir_ram INT8 NOT NULL,

    -- The UUID of the instance to which this VMM belongs.
    --
    -- This should eventually become NOT NULL for all VMMs, but is
    -- still nullable for backwards compatibility purposes. Specifically,
    -- the "instance start" saga can create rows in this table before creating
    -- rows for "omicron.public.vmm", which we would use for back-filling.
    -- If we tried to backfill + make this column non-nullable while that saga
    -- was mid-execution, we would still have some rows in this table with nullable
    -- values that would be more complex to fix.
    instance_id UUID
);

-- Allow looking up all VMM resources which reside on a sled
CREATE UNIQUE INDEX IF NOT EXISTS lookup_vmm_resource_by_sled ON omicron.public.sled_resource_vmm (
    sled_id,
    id
);

-- Allow looking up all resources by instance
CREATE INDEX IF NOT EXISTS lookup_vmm_resource_by_instance ON omicron.public.sled_resource_vmm (
    instance_id
);

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
    hw_baseboard_id UUID,

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
    subnet_octet INT2 NOT NULL UNIQUE CHECK (subnet_octet BETWEEN 33 AND 255),

    PRIMARY KEY (hw_baseboard_id, sled_id)
);

-- Add an index which allows pagination by {rack_id, sled_id} pairs.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_allocation_by_rack_and_sled ON omicron.public.sled_underlay_subnet_allocation (
    rack_id,
    sled_id
);

/*
 * Switches
 */

CREATE TABLE IF NOT EXISTS omicron.public.switch (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Rack table */
    rack_id UUID NOT NULL,

    /* Baseboard information about the switch */
    serial_number STRING(63) NOT NULL,
    part_number STRING(63) NOT NULL,
    revision INT8 NOT NULL
);

/* Add an index which lets us look up switches on a rack */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_switch_by_rack ON omicron.public.switch (
    rack_id,
    id
) WHERE time_deleted IS NULL;

/*
 * Services
 */

CREATE TYPE IF NOT EXISTS omicron.public.service_kind AS ENUM (
  'clickhouse',
  'clickhouse_keeper',
  'clickhouse_server',
  'cockroach',
  'crucible',
  'crucible_pantry',
  'dendrite',
  'external_dns',
  'internal_dns',
  'nexus',
  'ntp',
  'oximeter',
  'tfport',
  'mgd'
);

CREATE TYPE IF NOT EXISTS omicron.public.physical_disk_kind AS ENUM (
  'm2',
  'u2'
);

-- The disposition for a particular physical disk.
-- This is updated by the operator, either explicitly through an operator API,
-- or implicitly when altering sled policy.
CREATE TYPE IF NOT EXISTS omicron.public.physical_disk_policy AS ENUM (
    -- The disk is in service, and new resources can be provisioned onto it.
    'in_service',
    -- The disk has been, or will be, removed from the rack, and it should be
    -- assumed that any resources currently on it are now permanently missing.
    'expunged'
);

-- The actual state of a physical disk. This is updated exclusively by Nexus.
--
-- Nexus's goal is to match the physical disk's state with the
-- operator-indicated policy. For example, if the policy is "expunged" and the
-- state is "active", Nexus will assume that the physical disk is gone. Based
-- on that, Nexus will reallocate resources currently on the expunged disk
-- elsewhere, etc. Once the expunged disk no longer has any resources attached
-- to it, Nexus will mark it as decommissioned.
CREATE TYPE IF NOT EXISTS omicron.public.physical_disk_state AS ENUM (
    -- The disk has resources of any kind allocated on it, or, is available for
    -- new resources.
    --
    -- The disk can be in this state and have a different policy, e.g.
    -- "expunged".
    'active',

    -- The disk no longer has resources allocated on it, now or in the future.
    --
    -- This is a terminal state. This state is only valid if the policy is
    -- 'expunged'.
    'decommissioned'
);

-- A physical disk which exists inside the rack.
--
-- This is currently limited to U.2 disks, which are managed by the
-- control plane. A disk may exist within inventory, but not in this row:
-- if that's the case, it is not explicitly "managed" by Nexus.
CREATE TABLE IF NOT EXISTS omicron.public.physical_disk (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    vendor STRING(63) NOT NULL,
    serial STRING(63) NOT NULL,
    model STRING(63) NOT NULL,

    variant omicron.public.physical_disk_kind NOT NULL,

    -- FK into the Sled table
    sled_id UUID NOT NULL,

    disk_policy omicron.public.physical_disk_policy NOT NULL,
    disk_state omicron.public.physical_disk_state NOT NULL,

    -- This table should be limited to U.2s, and disallow inserting
    -- other disk kinds, unless we explicitly want them to be controlled
    -- by Nexus.
    --
    -- See https://github.com/oxidecomputer/omicron/issues/8258 for additional
    -- context.
    CONSTRAINT physical_disk_variant_u2 CHECK (variant = 'u2')
);

-- This constraint only needs to be upheld for disks that are not deleted
-- nor decommissioned.
CREATE UNIQUE INDEX IF NOT EXISTS vendor_serial_model_unique on omicron.public.physical_disk (
  vendor, serial, model
) WHERE time_deleted IS NULL AND disk_state != 'decommissioned';

CREATE UNIQUE INDEX IF NOT EXISTS lookup_physical_disk_by_variant ON omicron.public.physical_disk (
    variant,
    id
) WHERE time_deleted IS NULL;

-- Make it efficient to look up physical disks by Sled.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_physical_disk_by_sled ON omicron.public.physical_disk (
    sled_id,
    id
);

-- x509 certificates which may be used by services
CREATE TABLE IF NOT EXISTS omicron.public.certificate (
    -- Identity metadata (resource)
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- which Silo this certificate is used for
    silo_id UUID NOT NULL,

    -- The service type which should use this certificate
    service omicron.public.service_kind NOT NULL,

    -- cert.pem file (certificate chain in PEM format) as a binary blob
    cert BYTES NOT NULL,

    -- key.pem file (private key in PEM format) as a binary blob
    key BYTES NOT NULL
);

-- Add an index which lets us look up certificates for a particular service
-- class.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_certificate_by_service ON omicron.public.certificate (
    service,
    id
) WHERE
    time_deleted IS NULL;

-- Add an index which enforces that certificates have unique names, and which
-- allows pagination-by-name.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_certificate_by_silo ON omicron.public.certificate (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

-- A table describing virtual resource provisioning which may be associated
-- with a collection of objects, including:
-- - Projects
-- - Silos
-- - Fleet
CREATE TABLE IF NOT EXISTS omicron.public.virtual_provisioning_collection (
    -- Should match the UUID of the corresponding collection.
    id UUID PRIMARY KEY,
    time_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Identifies the type of the collection.
    collection_type STRING(63) NOT NULL,

    -- The amount of physical disk space which has been provisioned
    -- on behalf of the collection.
    virtual_disk_bytes_provisioned INT8 NOT NULL,

    -- The number of CPUs provisioned by VMs.
    cpus_provisioned INT8 NOT NULL,

    -- The amount of RAM provisioned by VMs.
    ram_provisioned INT8 NOT NULL
);

-- A table describing a single virtual resource which has been provisioned.
-- This may include:
-- - Disks
-- - Instances
-- - Snapshots
--
-- NOTE: You might think to yourself: "This table looks an awful lot like
-- the 'virtual_provisioning_collection' table, could they be condensed into
-- a single table?"
-- The answer to this question is unfortunately: "No". We use CTEs to both
-- UPDATE the collection table while INSERTing rows in the resource table, and
-- this would not be allowed if they came from the same table due to:
-- https://www.cockroachlabs.com/docs/v22.2/known-limitations#statements-containing-multiple-modification-subqueries-of-the-same-table-are-disallowed
-- However, by using separate tables, the CTE is able to function correctly.
CREATE TABLE IF NOT EXISTS omicron.public.virtual_provisioning_resource (
    -- Should match the UUID of the corresponding collection.
    id UUID PRIMARY KEY,
    time_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Identifies the type of the resource.
    resource_type STRING(63) NOT NULL,

    -- The amount of physical disk space which has been provisioned
    -- on behalf of the resource.
    virtual_disk_bytes_provisioned INT8 NOT NULL,

    -- The number of CPUs provisioned.
    cpus_provisioned INT8 NOT NULL,

    -- The amount of RAM provisioned.
    ram_provisioned INT8 NOT NULL
);

-- ZPools of Storage, attached to Sleds.
-- These are backed by a single physical disk.
--
-- For information about the provisioned zpool, reference the
-- "omicron.public.inv_zpool" table, which returns information
-- that has actually been returned from the underlying sled.
CREATE TABLE IF NOT EXISTS omicron.public.zpool (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Sled table */
    sled_id UUID NOT NULL,

    /* FK into the Physical Disk table */
    physical_disk_id UUID NOT NULL,

    /*
     * How many bytes to reserve for non-Crucible control plane storage
     */
    control_plane_storage_buffer INT NOT NULL
);

/* Create an index on the physical disk id */
CREATE INDEX IF NOT EXISTS lookup_zpool_by_disk on omicron.public.zpool (
    physical_disk_id,
    id
) WHERE physical_disk_id IS NOT NULL AND time_deleted IS NULL;

-- TODO-cleanup If modifying this enum, please remove 'update'; see
-- https://github.com/oxidecomputer/omicron/issues/8268.
CREATE TYPE IF NOT EXISTS omicron.public.dataset_kind AS ENUM (
  'crucible',
  'cockroach',
  'clickhouse',
  'clickhouse_keeper',
  'clickhouse_server',
  'external_dns',
  'internal_dns',
  'zone_root',
  'zone',
  'debug',
  'update'
);

/*
 * Table tracking the contact information and size used by datasets associated
 * with Crucible zones.
 *
 * This is a Reconfigurator rendezvous table: it reflects resources that
 * Reconfigurator has ensured exist. It is always possible that a resource
 * chosen from this table could be deleted after it's selected, but any
 * non-deleted row in this table is guaranteed to have been created.
 */
CREATE TABLE IF NOT EXISTS omicron.public.crucible_dataset (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Pool table */
    pool_id UUID NOT NULL,

    /*
     * Contact information for the dataset: socket address of the Crucible
     * agent service that owns this dataset
     */
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    /*
     * An upper bound on the amount of space that might be in-use
     *
     * This field is owned by Nexus. When a new row is inserted during the
     * Reconfigurator rendezvous process, this field is set to 0. Reconfigurator
     * otherwise ignores this field. It's updated by Nexus as region allocations
     * and deletions are performed using this dataset.
     *
     * Note that the value in this column is _not_ the sum of requested region
     * sizes, but sum of the size *reserved* by the Crucible agent for the
     * dataset that contains the regions (which is larger than the the actual
     * region size).
     */
    size_used INT NOT NULL,

    /* Do not consider this dataset during region allocation */
    no_provision BOOL NOT NULL
);

/* Create an index on the size usage for any Crucible dataset */
CREATE INDEX IF NOT EXISTS lookup_crucible_dataset_by_size_used ON
    omicron.public.crucible_dataset (size_used)
  WHERE time_deleted IS NULL;

/* Create an index on the zpool id */
CREATE INDEX IF NOT EXISTS lookup_crucible_dataset_by_zpool ON
    omicron.public.crucible_dataset (pool_id, id)
  WHERE time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS lookup_crucible_dataset_by_ip ON
  omicron.public.crucible_dataset (ip);

CREATE TYPE IF NOT EXISTS omicron.public.region_reservation_percent AS ENUM (
  '25'
);

/*
 * A region of space allocated to Crucible Downstairs, within a dataset.
 */
CREATE TABLE IF NOT EXISTS omicron.public.region (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /* FK into the dataset table */
    dataset_id UUID NOT NULL,

    /* FK into the volume table */
    volume_id UUID NOT NULL,

    /* Metadata describing the region */
    block_size INT NOT NULL,
    blocks_per_extent INT NOT NULL,
    extent_count INT NOT NULL,

    port INT4,

    read_only BOOL NOT NULL,

    deleting BOOL NOT NULL,

    /*
     * The Crucible Agent will reserve space for a region with overhead for
     * on-disk metadata that the downstairs needs to store. Record here the
     * overhead associated with a specific region as this may change or be
     * configurable in the future.
     */
    reservation_percent omicron.public.region_reservation_percent NOT NULL
);

/*
 * Allow all regions belonging to a disk to be accessed quickly.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_region_by_volume on omicron.public.region (
    volume_id,
    id
);

/*
 * Allow all regions belonging to a dataset to be accessed quickly.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_region_by_dataset on omicron.public.region (
    dataset_id,
    id
);

CREATE INDEX IF NOT EXISTS lookup_regions_missing_ports
    on omicron.public.region (id)
    WHERE port IS NULL;

CREATE INDEX IF NOT EXISTS lookup_regions_by_read_only
    on omicron.public.region (read_only);

/*
 * A snapshot of a region, within a dataset.
 */
CREATE TABLE IF NOT EXISTS omicron.public.region_snapshot (
    dataset_id UUID NOT NULL,
    region_id UUID NOT NULL,

    /* Associated higher level virtual snapshot */
    snapshot_id UUID NOT NULL,

    /*
     * Target string, for identification as part of
     * volume construction request(s)
     */
    snapshot_addr TEXT NOT NULL,

    /* How many volumes reference this? */
    volume_references INT8 NOT NULL,

    /* Is this currently part of some resources_to_delete? */
    deleting BOOL NOT NULL,

    PRIMARY KEY (dataset_id, region_id, snapshot_id)
);

/* Indexes for use during join with region table */
CREATE INDEX IF NOT EXISTS lookup_region_by_dataset on omicron.public.region_snapshot (
    dataset_id, region_id
);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_region_id on omicron.public.region_snapshot (
    region_id
);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_deleting on omicron.public.region_snapshot (
    deleting
);

/*
 * Index on volume_references and snapshot_addr for crucible
 * resource accounting lookup
 */
CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_volume_reference on omicron.public.region_snapshot (
    volume_references
);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_snapshot_addr on omicron.public.region_snapshot (
    snapshot_addr
);

/*
 * A volume within Crucible
 */
CREATE TABLE IF NOT EXISTS omicron.public.volume (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL,

    /*
     * A JSON document describing the construction of the volume, including all
     * sub volumes. This is what will be POSTed to propolis, and eventually
     * consumed by some Upstairs code to perform the volume creation. The Rust
     * type of this column should be Crucible::VolumeConstructionRequest.
     */
    data TEXT NOT NULL,

    /*
     * A JSON document describing what resources to clean up when deleting this
     * volume. The Rust type of this column should be the CrucibleResources
     * enum.
     */
    resources_to_clean_up TEXT
);

/* Quickly find deleted volumes */
CREATE INDEX IF NOT EXISTS lookup_volume_by_deleted on omicron.public.volume (
    time_deleted
);

/*
 * Silos
 */

CREATE TYPE IF NOT EXISTS omicron.public.authentication_mode AS ENUM (
  'local',
  'saml'
);

CREATE TYPE IF NOT EXISTS omicron.public.user_provision_type AS ENUM (
  'api_only',
  'jit',
  'scim'
);

CREATE TABLE IF NOT EXISTS omicron.public.silo (
    /* Identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    discoverable BOOL NOT NULL,
    authentication_mode omicron.public.authentication_mode NOT NULL,
    user_provision_type omicron.public.user_provision_type NOT NULL,

    mapped_fleet_roles JSONB NOT NULL,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL,

    admin_group_name TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_silo_by_name ON omicron.public.silo (
    name
) WHERE
    time_deleted IS NULL;

/*
 * Silo users
 */
CREATE TABLE IF NOT EXISTS omicron.public.silo_user (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,

    -- if the user provision type is 'api_only' or 'jit', then this field must
    -- contain a value
    external_id TEXT,

    user_provision_type omicron.public.user_provision_type,

    -- if the user provision type is 'scim' then this field must contain a value
    user_name TEXT,

    -- if user provision type is 'scim', this field _may_ contain a value: it
    -- is not mandatory that the SCIM provisioning client support this field.
    active BOOL,

    CONSTRAINT user_provision_type_required_for_non_deleted CHECK (
      (user_provision_type IS NOT NULL AND time_deleted IS NULL)
      OR (time_deleted IS NOT NULL)
    ),

    CONSTRAINT external_id_consistency CHECK (
        CASE user_provision_type
          WHEN 'api_only' THEN external_id IS NOT NULL
          WHEN 'jit' THEN external_id IS NOT NULL
        END
    ),

    CONSTRAINT user_name_consistency CHECK (
        CASE user_provision_type
          WHEN 'scim' THEN user_name IS NOT NULL
        END
    )
);

/* These indexes let us quickly find users for a given silo, and prevents
   multiple users from having the same provision specific unique identifier. */
CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_user_by_silo_and_external_id
ON
  omicron.public.silo_user (silo_id, external_id)
WHERE
  time_deleted IS NULL AND
  (user_provision_type = 'api_only' OR user_provision_type = 'jit');

CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_user_by_silo_and_user_name
ON
  omicron.public.silo_user (silo_id, user_name)
WHERE
  time_deleted IS NULL AND user_provision_type = 'scim';

CREATE TABLE IF NOT EXISTS omicron.public.silo_user_password_hash (
    silo_user_id UUID NOT NULL,
    hash TEXT NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY(silo_user_id)
);

/*
 * Silo groups
 */

CREATE TABLE IF NOT EXISTS omicron.public.silo_group (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,

    -- if the user provision type is 'api_only' or 'jit', then this field must
    -- contain a value
    external_id TEXT,

    user_provision_type omicron.public.user_provision_type,

    -- if the user provision type is 'scim' then this field must contain a value
    display_name TEXT,

    CONSTRAINT user_provision_type_required_for_non_deleted CHECK (
      (user_provision_type IS NOT NULL AND time_deleted IS NULL)
      OR (time_deleted IS NOT NULL)
    ),

    CONSTRAINT external_id_consistency CHECK (
        CASE user_provision_type
          WHEN 'api_only' THEN external_id IS NOT NULL
          WHEN 'jit' THEN external_id IS NOT NULL
        END
    ),

    CONSTRAINT display_name_consistency CHECK (
        CASE user_provision_type
          WHEN 'scim' THEN display_name IS NOT NULL
        END
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_group_by_silo_and_external_id
ON
  omicron.public.silo_group (silo_id, external_id)
WHERE
  time_deleted IS NULL and
  (user_provision_type = 'api_only' OR user_provision_type = 'jit');

CREATE UNIQUE INDEX IF NOT EXISTS
  lookup_silo_group_by_silo_and_display_name
ON
  omicron.public.silo_group (silo_id, display_name)
WHERE
  time_deleted IS NULL AND user_provision_type = 'scim';

/*
 * Silo group membership
 */

CREATE TABLE IF NOT EXISTS omicron.public.silo_group_membership (
    silo_group_id UUID NOT NULL,
    silo_user_id UUID NOT NULL,

    PRIMARY KEY (silo_group_id, silo_user_id)
);

/*
 * The primary key lets us paginate through the users in a group.  We need to
 * index the same fields in the reverse order to be able to paginate through the
 * groups that a user is in.
 */
CREATE INDEX IF NOT EXISTS lookup_silo_group_by_user ON omicron.public.silo_group_membership (
    silo_user_id,
    silo_group_id
);

/*
 * Silo identity provider list
 */

CREATE TYPE IF NOT EXISTS omicron.public.provider_type AS ENUM (
  'saml'
);

CREATE TABLE IF NOT EXISTS omicron.public.identity_provider (
    /* Identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,
    provider_type omicron.public.provider_type NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_idp_by_silo_id ON omicron.public.identity_provider (
    silo_id,
    id
) WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_idp_by_silo_name ON omicron.public.identity_provider (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Silo SAML identity provider
 */
CREATE TABLE IF NOT EXISTS omicron.public.saml_identity_provider (
    /* Identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,

    idp_metadata_document_string TEXT NOT NULL,

    idp_entity_id TEXT NOT NULL,
    sp_client_id TEXT NOT NULL,
    acs_url TEXT NOT NULL,
    slo_url TEXT NOT NULL,
    technical_contact_email TEXT NOT NULL,

    public_cert TEXT,
    private_key TEXT,

    group_attribute_name TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_saml_idp_by_silo_id ON omicron.public.saml_identity_provider (
    silo_id,
    id
) WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_saml_idp_by_silo_name ON omicron.public.saml_identity_provider (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Users' public SSH keys, per RFD 44
 */
CREATE TABLE IF NOT EXISTS omicron.public.ssh_key (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* FK into silo_user table */
    silo_user_id UUID NOT NULL,

    /*
     * A 4096 bit RSA key without comment encodes to 726 ASCII characters.
     * A (256 bit) Ed25519 key w/o comment encodes to 82 ASCII characters.
     */
    public_key STRING(1023) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_ssh_key_by_silo_user ON omicron.public.ssh_key (
    silo_user_id,
    name
) WHERE
    time_deleted IS NULL;

/**
 * Represents the SSH keys copied to an instance at create time by cloud-init.
 * Entries are added here when an instance is created (with configured SSH keys)
 * and removed when the instance is destroyed.
 *
 * TODO: Should this have time created / time deleted
 */
CREATE TABLE IF NOT EXISTS omicron.public.instance_ssh_key (
    instance_id UUID NOT NULL,
    ssh_key_id UUID NOT NULL,
    PRIMARY KEY (instance_id, ssh_key_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.silo_quotas (
    silo_id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    cpus INT8 NOT NULL,
    memory_bytes INT8 NOT NULL,
    storage_bytes INT8 NOT NULL,

    CONSTRAINT cpus_not_negative CHECK (cpus >= 0),
    CONSTRAINT memory_not_negative CHECK (memory_bytes >= 0),
    CONSTRAINT storage_not_negative CHECK (storage_bytes >= 0)
);

/**
 * A view of the amount of provisioned and allocated (set by quotas) resources
 * on a given silo.
 */
CREATE VIEW IF NOT EXISTS omicron.public.silo_utilization
AS SELECT
    c.id AS silo_id,
    s.name AS silo_name,
    c.cpus_provisioned AS cpus_provisioned,
    c.ram_provisioned AS memory_provisioned,
    c.virtual_disk_bytes_provisioned AS storage_provisioned,
    q.cpus AS cpus_allocated,
    q.memory_bytes AS memory_allocated,
    q.storage_bytes AS storage_allocated,
    s.discoverable as silo_discoverable
FROM
    omicron.public.virtual_provisioning_collection AS c
    RIGHT JOIN omicron.public.silo_quotas AS q
    ON c.id = q.silo_id
    INNER JOIN omicron.public.silo AS s
    ON c.id = s.id
WHERE
    c.collection_type = 'Silo'
AND
    s.time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.silo_auth_settings (
    silo_id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- null means no max: users can tokens that never expire
    device_token_max_ttl_seconds INT8 CHECK (device_token_max_ttl_seconds > 0)
);
/*
 * Projects
 */

CREATE TABLE IF NOT EXISTS omicron.public.project (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL,

    /* Which silo this project belongs to */
    silo_id UUID NOT NULL /* foreign key into "silo" table */
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_project_by_silo ON omicron.public.project (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Instances
 */

CREATE TYPE IF NOT EXISTS omicron.public.instance_state_v2 AS ENUM (
    /* The instance exists in the DB but its create saga is still in flight. */
    'creating',

    /*
     * The instance has no active VMM. Corresponds to the "stopped" external
     * state.
     */
    'no_vmm',

    /* The instance's state is derived from its active VMM's state. */
    'vmm',

    /* Something bad happened while trying to interact with the instance. */
    'failed',

    /* The instance has been destroyed. */
    'destroyed'
);

CREATE TYPE IF NOT EXISTS omicron.public.vmm_state AS ENUM (
    /*
     * The VMM is known to Nexus, but may not yet exist on a sled.
     *
     * VMM records are always inserted into the database in this state, and
     * then transition to 'starting' or 'migrating' once a sled-agent reports
     * that the VMM has been registered.
     */
    'creating',
    'starting',
    'running',
    'stopping',
    'stopped',
    'rebooting',
    'migrating',
    'failed',
    'destroyed',
    'saga_unwound'
);

CREATE TYPE IF NOT EXISTS omicron.public.instance_auto_restart AS ENUM (
    /*
     * The instance should not, under any circumstances, be automatically
     * rebooted by the control plane.
     */
    'never',
    /*
     * If this instance is running and unexpectedly fails (e.g. due to a host
     * software crash or unexpected host reboot), the control plane will make a
     * best-effort attempt to restart it. The control plane may choose not to
     * restart the instance to preserve the overall availability of the system.
     */
     'best_effort'
);

CREATE TYPE IF NOT EXISTS omicron.public.instance_cpu_platform AS ENUM (
  'amd_milan',
  'amd_turin'
);

/*
 * Represents the *desired* state of an instance, as requested by the user.
 */
CREATE TYPE IF NOT EXISTS omicron.public.instance_intended_state AS ENUM (
    /* The instance should be running. */
    'running',

    /* The instance was asked to stop by an API request. */
    'stopped',

    /* The guest OS shut down the virtual machine.
     *
     * This is distinct from the 'stopped' intent, which represents a stop
     * requested by the API.
     */
    'guest_shutdown',

    /* The instance should be destroyed. */
    'destroyed'
);

/*
 * TODO consider how we want to manage multiple sagas operating on the same
 * Instance -- e.g., reboot concurrent with destroy or concurrent reboots or the
 * like.  Or changing # of CPUs or memory size.
 */
CREATE TABLE IF NOT EXISTS omicron.public.instance (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    /* This is redundant for Instances, but we keep it here for consistency. */
    time_deleted TIMESTAMPTZ,

    /* Every Instance is in exactly one Project at a time. */
    project_id UUID NOT NULL,

    /* user data for instance initialization systems (e.g. cloud-init) */
    user_data BYTES NOT NULL,

    /* The last-updated time and generation for the instance's state. */
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,

    /* FK into `vmm` for the Propolis server that's backing this instance. */
    active_propolis_id UUID,

    /* FK into `vmm` for the migration target Propolis server, if one exists. */
    target_propolis_id UUID,

    /* Identifies any ongoing migration for this instance. */
    migration_id UUID,

    /* Instance configuration */
    ncpus INT NOT NULL,
    memory INT NOT NULL,
    hostname STRING(63) NOT NULL,

    /* ID of the instance update saga that has locked this instance for
     * updating, if one exists. */
    updater_id UUID,

    /* Generation of the instance updater lock */
    updater_gen INT NOT NULL DEFAULT 0,

    /*
     * The internal instance state. If this is 'vmm', the externally-visible
     * instance state is derived from its active VMM's state. This column is
     * distant from its generation number and update time because it is
     * deleted and recreated by the schema upgrade process; see the
     * `separate-instance-and-vmm-states` schema change for details.
     */
    state omicron.public.instance_state_v2 NOT NULL,

    /*
     * The time of the most recent auto-restart attempt, or NULL if the control
     * plane has never attempted to automatically restart this instance.
     */
    time_last_auto_restarted TIMESTAMPTZ,

    /*
     * What failures should result in an instance being automatically restarted
     * by the control plane.
     */
    auto_restart_policy omicron.public.instance_auto_restart,
    /*
     * The cooldown period that must elapse between consecutive auto restart
     * attempts. If this is NULL, no cooldown period is explicitly configured
     * for this instance, and the default cooldown period should be used.
     */
     auto_restart_cooldown INTERVAL,

    /*
     * Which disk, if any, is the one this instance should be directed to boot
     * from. With a boot device selected, guest OSes cannot configure their
     * boot policy for future boots, so also permit NULL to indicate a guest
     * does not want our policy, and instead should be permitted control over
     * its boot-time fates.
     */
    boot_disk_id UUID,

    /*
     * The intended state of the instance, as requested by the user.
     *
     * This may differ from its current state, and is used to determine what
     * action should be taken when the instance's VMM state changes.
     */
    intended_state omicron.public.instance_intended_state NOT NULL,

    /*
     * The required CPU platform for this instance. If set, the instance's VMs
     * may see additional features present in that platform, but in exchange
     * they may only run on sleds whose CPUs support all of those features.
     *
     * If this is NULL, the control plane ignores CPU constraints when selecting
     * a sled for this instance. Then, once it has selected a sled, it supplies
     * a "lowest common denominator" CPU platform that is compatible with that
     * sled to maximize the number of sleds the VM can migrate to.
     */
    cpu_platform omicron.public.instance_cpu_platform,

    CONSTRAINT vmm_iff_active_propolis CHECK (
        ((state = 'vmm') AND (active_propolis_id IS NOT NULL)) OR
        ((state != 'vmm') AND (active_propolis_id IS NULL))
    )
);

-- Names for instances within a project should be unique
CREATE UNIQUE INDEX IF NOT EXISTS lookup_instance_by_project ON omicron.public.instance (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

-- Many control plane operations wish to select all the instances in particular
-- states.
CREATE INDEX IF NOT EXISTS lookup_instance_by_state
ON
    omicron.public.instance (state)
WHERE
    time_deleted IS NULL;

/*
 * A special view of an instance provided to operators for insights into what's running
 * on a sled.
 *
 * This view requires the VMM table, which doesn't exist yet, so create a
 * "placeholder" view here and replace it with the full view once the table is
 * defined. See the README for more context.
 */

CREATE VIEW IF NOT EXISTS omicron.public.sled_instance
AS SELECT
    instance.id
FROM
    omicron.public.instance AS instance
WHERE
    instance.time_deleted IS NULL;

/*
 * Guest-Visible, Virtual Disks
 */

/*
 * TODO The Rust enum to which this type is converted
 * carries data in some of its variants, such as the UUID
 * of the instance to which a disk is attached.
 *
 * This makes the conversion to/from this enum type here much
 * more difficult, since we need a way to manage that data
 * coherently.
 *
 * See <https://github.com/oxidecomputer/omicron/issues/312>.
 */
-- CREATE TYPE omicron.public.DiskState AS ENUM (
--     'creating',
--     'detached',
--     'attaching',
--     'attached',
--     'detaching',
--     'destroyed',
--     'faulted'
-- );

CREATE TYPE IF NOT EXISTS omicron.public.block_size AS ENUM (
  '512',
  '2048',
  '4096'
);

CREATE TABLE IF NOT EXISTS omicron.public.disk (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    /* This is redundant for Disks, but we keep it here for consistency. */
    time_deleted TIMESTAMPTZ,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL,

    /* Every Disk is in exactly one Project at a time. */
    project_id UUID NOT NULL,

    /* Every disk consists of a root volume */
    volume_id UUID NOT NULL,

    /*
     * TODO Would it make sense for the runtime state to live in a separate
     * table?
     */
    /* Runtime state */
    -- disk_state omicron.public.DiskState NOT NULL, /* TODO see above */
    disk_state STRING(32) NOT NULL,
    /*
     * Every Disk may be attaching to, attached to, or detaching from at most
     * one Instance at a time.
     */
    attach_instance_id UUID,
    state_generation INT NOT NULL,
    slot INT2 CHECK (slot >= 0 AND slot < 8),
    time_state_updated TIMESTAMPTZ NOT NULL,

    /* Disk configuration */
    size_bytes INT NOT NULL,
    block_size omicron.public.block_size NOT NULL,
    origin_snapshot UUID,
    origin_image UUID,

    pantry_address TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_disk_by_project ON omicron.public.disk (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_disk_by_instance ON omicron.public.disk (
    attach_instance_id,
    id
) WHERE
    time_deleted IS NULL AND attach_instance_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_deleted_disk ON omicron.public.disk (
    id
) WHERE
    time_deleted IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_disk_by_volume_id ON omicron.public.disk (
    volume_id
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.image (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,
    project_id UUID,

    volume_id UUID NOT NULL,

    url STRING(8192),
    os STRING(64) NOT NULL,
    version STRING(64) NOT NULL,
    digest TEXT,
    block_size omicron.public.block_size NOT NULL,
    size_bytes INT NOT NULL
);

CREATE VIEW IF NOT EXISTS omicron.public.project_image AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    silo_id,
    project_id,
    volume_id,
    url,
    os,
    version,
    digest,
    block_size,
    size_bytes
FROM
    omicron.public.image
WHERE
    project_id IS NOT NULL;

CREATE VIEW IF NOT EXISTS omicron.public.silo_image AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    silo_id,
    volume_id,
    url,
    os,
    version,
    digest,
    block_size,
    size_bytes
FROM
    omicron.public.image
WHERE
    project_id IS NULL;

/* Index for silo images */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_image_by_silo on omicron.public.image (
    silo_id,
    name
) WHERE
    time_deleted is NULL AND
    project_id is NULL;

/* Index for project images */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_image_by_silo_and_project on omicron.public.image (
    silo_id,
    project_id,
    name
) WHERE
    time_deleted is NULL AND
    project_id is NOT NULL;

CREATE TYPE IF NOT EXISTS omicron.public.snapshot_state AS ENUM (
  'creating',
  'ready',
  'faulted',
  'destroyed'
);

CREATE TABLE IF NOT EXISTS omicron.public.snapshot (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* Every Snapshot is in exactly one Project at a time. */
    project_id UUID NOT NULL,

    /* Every Snapshot originated from a single disk */
    disk_id UUID NOT NULL,

    /* Every Snapshot consists of a root volume */
    volume_id UUID NOT NULL,

    /* Where will the scrubbed blocks eventually land? */
    destination_volume_id UUID NOT NULL,

    gen INT NOT NULL,
    state omicron.public.snapshot_state NOT NULL,
    block_size omicron.public.block_size NOT NULL,

    /* Disk configuration (from the time the snapshot was taken) */
    size_bytes INT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_snapshot_by_project
    ON omicron.public.snapshot (
        project_id,
        name
    ) WHERE
        time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS lookup_snapshot_by_destination_volume_id
    ON omicron.public.snapshot ( destination_volume_id );

CREATE INDEX IF NOT EXISTS lookup_snapshot_by_volume_id
    ON omicron.public.snapshot ( volume_id );

/*
 * Oximeter collector servers.
 */
CREATE TABLE IF NOT EXISTS omicron.public.oximeter (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,
    time_expunged TIMESTAMPTZ
);

/*
 * The query Nexus runs to choose an Oximeter instance for new metric producers
 * involves listing the non-expunged instances sorted by ID, which would require
 * a full table scan without this index.
 */
CREATE UNIQUE INDEX IF NOT EXISTS list_non_expunged_oximeter ON omicron.public.oximeter (
    id
) WHERE
    time_expunged IS NULL;

/*
 * The kind of metric producer each record corresponds to.
 */
CREATE TYPE IF NOT EXISTS omicron.public.producer_kind AS ENUM (
    -- A sled agent for an entry in the sled table.
    'sled_agent',
    -- A service in a blueprint (typically the current target blueprint, but it
    -- may reference a prior blueprint if the service is in the process of being
    -- removed).
    'service',
    -- A Propolis VMM for an instance in the omicron.public.instance table
    'instance',
    -- A management gateway service on a scrimlet.
    'management_gateway'
);

/*
 * Information about registered metric producers.
 */
CREATE TABLE IF NOT EXISTS omicron.public.metric_producer (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    kind omicron.public.producer_kind NOT NULL,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,
    interval FLOAT NOT NULL,
    /* Oximeter collector instance to which this metric producer is assigned. */
    oximeter_id UUID NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_producer_by_oximeter ON omicron.public.metric_producer (
    oximeter_id,
    id
);

CREATE INDEX IF NOT EXISTS lookup_producer_by_time_modified ON omicron.public.metric_producer (
    time_modified
);

/*
 * VPCs and networking primitives
 */


CREATE TABLE IF NOT EXISTS omicron.public.vpc (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,
    project_id UUID NOT NULL,
    system_router_id UUID NOT NULL,
    dns_name STRING(63) NOT NULL,

    /*
     * The Geneve Virtual Network Identifier for this VPC. Note that this is a
     * 24-bit unsigned value, properties which are checked in the application,
     * not the database.
     */
    vni INT4 NOT NULL,

    /* The IPv6 prefix allocated to subnets. */
    ipv6_prefix INET NOT NULL,

    /* Used to ensure that two requests do not concurrently modify the
       VPC's firewall */
    firewall_gen INT NOT NULL,

    /* Child-resource generation number for VPC Subnets. */
    subnet_gen INT8 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_vpc_by_project ON omicron.public.vpc (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_vpc_by_vni ON omicron.public.vpc (
    vni
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.vpc_subnet (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,
    vpc_id UUID NOT NULL,
    /* Child resource creation generation number */
    rcgen INT8 NOT NULL,
    ipv4_block INET NOT NULL,
    ipv6_block INET NOT NULL,
    /* nullable FK to the `vpc_router` table. */
    custom_router_id UUID
);

/* Subnet and network interface names are unique per VPC, not project */
CREATE UNIQUE INDEX IF NOT EXISTS vpc_subnet_vpc_id_name_key ON omicron.public.vpc_subnet (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

/* The kind of network interface. */
CREATE TYPE IF NOT EXISTS omicron.public.network_interface_kind AS ENUM (
    /* An interface attached to a guest instance. */
    'instance',

    /* An interface attached to a service. */
    'service',
    'probe'
);

CREATE TABLE IF NOT EXISTS omicron.public.network_interface (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* The kind of network interface, e.g., instance */
    kind omicron.public.network_interface_kind NOT NULL,

    /*
     * FK into the parent resource of this interface (e.g. Instance, Service)
     * as determined by the `kind`.
     */
    parent_id UUID NOT NULL,

    /* FK into VPC table */
    vpc_id UUID NOT NULL,
    /* FK into VPCSubnet table. */
    subnet_id UUID NOT NULL,

    /*
     * The EUI-48 MAC address of the guest interface.
     *
     * Note that we use the bytes of a 64-bit integer, in big-endian byte order
     * to represent the MAC.
     */
    mac INT8 NOT NULL,

    /* The private VPC IP address of the interface. */
    ip INET NOT NULL,

    /*
     * Limited to 8 NICs per instance. This value must be kept in sync with
     * `crate::nexus::MAX_NICS_PER_INSTANCE`.
     */
    slot INT2 NOT NULL CHECK (slot >= 0 AND slot < 8),

    /* True if this interface is the primary interface.
     *
     * The primary interface appears in DNS and its address is used for external
     * connectivity.
     */
    is_primary BOOL NOT NULL,

    /*
     * A supplementary list of addresses/CIDR blocks which a NIC is
     * *allowed* to send/receive traffic on, in addition to its
     * assigned address.
     */
    transit_ips INET[] NOT NULL DEFAULT ARRAY[]
);

CREATE INDEX IF NOT EXISTS instance_network_interface_mac
    ON omicron.public.network_interface (mac) STORING (time_deleted);

/* A view of the network_interface table for just instance-kind records. */
CREATE VIEW IF NOT EXISTS omicron.public.instance_network_interface AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    parent_id AS instance_id,
    vpc_id,
    subnet_id,
    mac,
    ip,
    slot,
    is_primary,
    transit_ips
FROM
    omicron.public.network_interface
WHERE
    kind = 'instance';

/* A view of the network_interface table for just service-kind records. */
CREATE VIEW IF NOT EXISTS omicron.public.service_network_interface AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    parent_id AS service_id,
    vpc_id,
    subnet_id,
    mac,
    ip,
    slot,
    is_primary
FROM
    omicron.public.network_interface
WHERE
    kind = 'service';

/* TODO-completeness

 * We currently have a NetworkInterface table with the IP and MAC addresses inline.
 * Eventually, we'll probably want to move these to their own tables, and
 * refer to them here, most notably to support multiple IPs per NIC, as well
 * as moving IPs between NICs on different instances, etc.
 */

/* Ensure we do not assign the same address twice within a subnet */
CREATE UNIQUE INDEX IF NOT EXISTS network_interface_subnet_id_ip_key ON omicron.public.network_interface (
    subnet_id,
    ip
) WHERE
    time_deleted IS NULL;

/* Ensure we do not assign the same MAC twice within a VPC
 * See RFD174's discussion on the scope of virtual MACs
 */
CREATE UNIQUE INDEX IF NOT EXISTS network_interface_vpc_id_mac_key ON omicron.public.network_interface (
    vpc_id,
    mac
) WHERE
    time_deleted IS NULL;

/*
 * Index used to verify that all interfaces for a resource (e.g. Instance,
 * Service) are contained within a single VPC, and that all interfaces are
 * in unique VPC Subnets.
 *
 * This is also used to quickly find the primary interface since
 * we store the `is_primary` column. Such queries are mostly used
 * when setting a new primary interface.
 */
CREATE UNIQUE INDEX IF NOT EXISTS network_interface_parent_id_name_kind_key ON omicron.public.network_interface (
    parent_id,
    name,
    kind
)
STORING (vpc_id, subnet_id, is_primary)
WHERE
    time_deleted IS NULL;

/*
 * Index used to verify that all interfaces for a resource (e.g. Instance,
 * Service) have unique slots.
 */
CREATE UNIQUE INDEX IF NOT EXISTS network_interface_parent_id_slot_key ON omicron.public.network_interface (
    parent_id,
    slot
)
WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.vpc_firewall_rule_status AS ENUM (
    'disabled',
    'enabled'
);

CREATE TYPE IF NOT EXISTS omicron.public.vpc_firewall_rule_direction AS ENUM (
    'inbound',
    'outbound'
);

CREATE TYPE IF NOT EXISTS omicron.public.vpc_firewall_rule_action AS ENUM (
    'allow',
    'deny'
);

CREATE TABLE IF NOT EXISTS omicron.public.vpc_firewall_rule (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    vpc_id UUID NOT NULL,
    status omicron.public.vpc_firewall_rule_status NOT NULL,
    direction omicron.public.vpc_firewall_rule_direction NOT NULL,
    /* Array of targets. 128 was picked to include plenty of space for
       a tag, colon, and resource identifier. */
    targets STRING(128)[] NOT NULL,
    /* Also an array of targets */
    filter_hosts STRING(128)[],
    filter_ports STRING(11)[],
    action omicron.public.vpc_firewall_rule_action NOT NULL,
    priority INT4 CHECK (priority BETWEEN 0 AND 65535) NOT NULL,
    filter_protocols STRING(32)[]
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_firewall_by_vpc ON omicron.public.vpc_firewall_rule (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.vpc_router_kind AS ENUM (
    'system',
    'custom'
);

CREATE TABLE IF NOT EXISTS omicron.public.vpc_router (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,
    kind omicron.public.vpc_router_kind NOT NULL,
    vpc_id UUID NOT NULL,
    rcgen INT NOT NULL,
    /*
     * version information used to trigger VPC router RPW.
     * this is sensitive to CRUD on named resources beyond
     * routers e.g. instances, subnets, ...
     */
    resolved_version INT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_router_by_vpc ON omicron.public.vpc_router (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

/* Index used to accelerate vpc_increment_rpw_version and list. */
CREATE INDEX IF NOT EXISTS lookup_routers_in_vpc ON omicron.public.vpc_router (
    vpc_id
) WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.router_route_kind AS ENUM (
    'default',
    'vpc_subnet',
    'vpc_peering',
    'custom'
);

CREATE TABLE IF NOT EXISTS omicron.public.router_route (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* FK to the `vpc_router` table. */
    vpc_router_id UUID NOT NULL,
    kind omicron.public.router_route_kind NOT NULL,
    target STRING(128) NOT NULL,
    destination STRING(128) NOT NULL,

    /* FK to the `vpc_subnet` table. See constraints below */
    vpc_subnet_id UUID,

    /*
     * Only nullable if this is rule is not, in-fact, virtual and tightly coupled to a
     * linked item. Today, these are 'vpc_subnet' rules and their parent subnets.
     * 'vpc_peering' routes may also fall into this category in future.
     *
     * User-created/modifiable routes must have this field as NULL.
     */
    CONSTRAINT non_null_vpc_subnet CHECK (
        (kind = 'vpc_subnet' AND vpc_subnet_id IS NOT NULL) OR
        (kind != 'vpc_subnet' AND vpc_subnet_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_route_by_router ON omicron.public.router_route (
    vpc_router_id,
    name
) WHERE
    time_deleted IS NULL;

-- Enforce uniqueness of 'vpc_subnet' routes on parent (and help add/delete).
CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_route_by_id ON omicron.public.router_route (
    vpc_subnet_id
) WHERE
    time_deleted IS NULL AND kind = 'vpc_subnet';

CREATE TABLE IF NOT EXISTS omicron.public.internet_gateway (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    vpc_id UUID NOT NULL,
    rcgen INT NOT NULL,
    resolved_version INT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_internet_gateway_by_vpc ON omicron.public.internet_gateway (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.internet_gateway_ip_pool (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    internet_gateway_id UUID,
    ip_pool_id UUID
);

CREATE INDEX IF NOT EXISTS lookup_internet_gateway_ip_pool_by_igw_id ON omicron.public.internet_gateway_ip_pool (
    internet_gateway_id
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.internet_gateway_ip_address (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    internet_gateway_id UUID,
    address INET
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_internet_gateway_ip_address_by_igw_id ON omicron.public.internet_gateway_ip_address (
    internet_gateway_id
) WHERE
    time_deleted IS NULL;

/* The IP version of an IP address. */
CREATE TYPE IF NOT EXISTS omicron.public.ip_version AS ENUM (
    'v4',
    'v6'
);

/* Indicates what an IP Pool is reserved for. */
CREATE TYPE IF NOT EXISTS omicron.public.ip_pool_reservation_type AS ENUM (
    'external_silos',
    'oxide_internal'
);

/*
 * IP pool types for unicast vs multicast pools
 */
CREATE TYPE IF NOT EXISTS omicron.public.ip_pool_type AS ENUM (
    'unicast',
    'multicast'
);

/*
 * An IP Pool, a collection of zero or more IP ranges for external IPs.
 */
CREATE TABLE IF NOT EXISTS omicron.public.ip_pool (
    /* Resource identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* The collection's child-resource generation number */
    rcgen INT8 NOT NULL,

    /* The IP version of the ranges contained in this pool. */
    ip_version omicron.public.ip_version NOT NULL,

    /* Indicates what the IP Pool is reserved for. */
    reservation_type omicron.public.ip_pool_reservation_type NOT NULL,

    /* Pool type for unicast (default) vs multicast pools. */
    pool_type omicron.public.ip_pool_type NOT NULL DEFAULT 'unicast'
);

/*
 * Index ensuring uniqueness of IP Pool names, globally.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_by_name ON omicron.public.ip_pool (
    name
) WHERE
    time_deleted IS NULL;

/*
 * Index on pool type for efficient filtering of unicast vs multicast pools.
 */
CREATE INDEX IF NOT EXISTS lookup_ip_pool_by_type ON omicron.public.ip_pool (
    pool_type
) WHERE
    time_deleted IS NULL;

-- The order here is most-specific first, and it matters because we use this
-- fact to select the most specific default in the case where there is both a
-- silo default and a fleet default. If we were to add a project type, it should
-- be added before silo.
CREATE TYPE IF NOT EXISTS omicron.public.ip_pool_resource_type AS ENUM (
    'silo'
);

-- join table associating IP pools with resources like fleet or silo
CREATE TABLE IF NOT EXISTS omicron.public.ip_pool_resource (
    ip_pool_id UUID NOT NULL,
    resource_type omicron.public.ip_pool_resource_type NOT NULL,
    resource_id UUID NOT NULL,
    is_default BOOL NOT NULL,
    -- TODO: timestamps for soft deletes?

    -- resource_type is redundant because resource IDs are globally unique, but
    -- logically it belongs here
    PRIMARY KEY (ip_pool_id, resource_type, resource_id)
);

-- a given resource can only have one default ip pool
CREATE UNIQUE INDEX IF NOT EXISTS one_default_ip_pool_per_resource ON omicron.public.ip_pool_resource (
    resource_id
) where
    is_default = true;

-- created solely to prevent a table scan when we delete links on silo delete
CREATE INDEX IF NOT EXISTS ip_pool_resource_id ON omicron.public.ip_pool_resource (
    resource_id
);

CREATE INDEX IF NOT EXISTS ip_pool_resource_ip_pool_id ON omicron.public.ip_pool_resource (
    ip_pool_id
);

/*
 * IP Pools are made up of a set of IP ranges, which are start/stop addresses.
 * Note that these need not be CIDR blocks or well-behaved subnets with a
 * specific netmask.
 */
CREATE TABLE IF NOT EXISTS omicron.public.ip_pool_range (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    first_address INET NOT NULL,
    /* The range is inclusive of the last address. */
    last_address INET NOT NULL,
    /* FK into the `ip_pool` table. */
    ip_pool_id UUID NOT NULL,
    /* Tracks child resources, IP addresses allocated out of this range. */
    rcgen INT8 NOT NULL,

    /* Ensure first address is not greater than last address */
    CONSTRAINT check_address_order CHECK (first_address <= last_address)
);

/*
 * These help Nexus enforce that the ranges within an IP Pool do not overlap
 * with any other ranges. See `nexus/src/db/queries/ip_pool.rs` for the actual
 * query which does that.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_range_by_first_address ON omicron.public.ip_pool_range (
    first_address
)
STORING (last_address)
WHERE time_deleted IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_range_by_last_address ON omicron.public.ip_pool_range (
    last_address
)
STORING (first_address)
WHERE time_deleted IS NULL;


/* The kind of external IP address. */
CREATE TYPE IF NOT EXISTS omicron.public.ip_kind AS ENUM (
    /*
     * Source NAT provided to all guests by default or for services that
     * only require outbound external connectivity.
     */
    'snat',

    /*
     * An ephemeral IP is a fixed, known address whose lifetime is the same as
     * the instance to which it is attached.
     * Not valid for services.
     */
    'ephemeral',

    /*
     * A floating IP is an independent, named API resource that can be assigned
     * to an instance or service.
     */
    'floating'
);

CREATE TYPE IF NOT EXISTS omicron.public.ip_attach_state AS ENUM (
    'detached',
    'attached',
    'detaching',
    'attaching'
);

/*
 * External IP addresses used for guest instances and externally-facing
 * services.
 */
CREATE TABLE IF NOT EXISTS omicron.public.external_ip (
    /* Identity metadata */
    id UUID PRIMARY KEY,

    /* Name for floating IPs. See the constraints below. */
    name STRING(63),

    /* Description for floating IPs. See the constraints below. */
    description STRING(512),

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* FK to the `ip_pool` table. */
    ip_pool_id UUID NOT NULL,

    /* FK to the `ip_pool_range` table. */
    ip_pool_range_id UUID NOT NULL,

    /* True if this IP is associated with a service rather than an instance. */
    is_service BOOL NOT NULL,

    /* FK to the `instance` or `service` table. See constraints below. */
    parent_id UUID,

    /* The kind of external address, e.g., ephemeral. */
    kind omicron.public.ip_kind NOT NULL,

    /* The actual external IP address. */
    ip INET NOT NULL,

    /* The first port in the allowed range, inclusive. */
    first_port INT4 NOT NULL,

    /* The last port in the allowed range, also inclusive. */
    last_port INT4 NOT NULL,

    /* FK to the `project` table. */
    project_id UUID,

    /* State of this IP with regard to instance attach/detach
     * operations. This is mainly used to prevent concurrent use
     * across sagas and allow rollback to correct state.
     */
    state omicron.public.ip_attach_state NOT NULL,

    is_probe BOOL NOT NULL DEFAULT false,

    /* The name must be non-NULL iff this is a floating IP. */
    CONSTRAINT null_fip_name CHECK (
        (kind != 'floating' AND name IS NULL) OR
        (kind = 'floating' AND name IS NOT NULL)
    ),

    /* The description must be non-NULL iff this is a floating IP. */
    CONSTRAINT null_fip_description CHECK (
        (kind != 'floating' AND description IS NULL) OR
        (kind = 'floating' AND description IS NOT NULL)
    ),

    /* Only floating IPs can be attached to a project, and
     * they must have a parent project if they are instance FIPs.
     */
    CONSTRAINT null_project_id CHECK (
        (kind = 'floating' AND is_service = FALSE AND project_id is NOT NULL) OR
        ((kind != 'floating' OR is_service = TRUE) AND project_id IS NULL)
    ),

    /*
     * Only nullable if this is a floating/ephemeral IP, which may exist not
     * attached to any instance or service yet. Ephemeral IPs should not generally
     * exist without parent instances/services, but need to temporarily exist in
     * this state for live attachment.
     */
    CONSTRAINT null_snat_parent_id CHECK (
        (kind != 'snat') OR (parent_id IS NOT NULL)
    ),

    /* Ephemeral IPs are not supported for services. */
    CONSTRAINT ephemeral_kind_service CHECK (
        (kind = 'ephemeral' AND is_service = FALSE) OR (kind != 'ephemeral')
    ),

    /*
     * (Not detached) => non-null parent_id.
     * This is not a two-way implication because SNAT IPs
     * cannot have a null parent_id.
     */
    CONSTRAINT detached_null_parent_id CHECK (
        (state = 'detached') OR (parent_id IS NOT NULL)
    )
);

/*
 * Index used to support quickly looking up children of the IP Pool range table,
 * when checking for allocated addresses during deletion.
 */
CREATE INDEX IF NOT EXISTS external_ip_by_pool ON omicron.public.external_ip (
    ip_pool_id,
    ip_pool_range_id
)
    WHERE time_deleted IS NULL;

/*
 * Index used to enforce uniqueness of external IPs
 *
 * NOTE: This relies on the uniqueness constraint of IP addresses across all
 * pools, _and_ on the fact that the number of ports assigned to each instance
 * is fixed at compile time.
 */
CREATE UNIQUE INDEX IF NOT EXISTS external_ip_unique ON omicron.public.external_ip (
    ip,
    first_port
)
    WHERE time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_ip_by_parent ON omicron.public.external_ip (
    parent_id,
    id
)
    WHERE parent_id IS NOT NULL AND time_deleted IS NULL;

/* Enforce a limit of one Ephemeral IP per instance */
CREATE UNIQUE INDEX IF NOT EXISTS one_ephemeral_ip_per_instance ON omicron.public.external_ip (
    parent_id
)
    WHERE kind = 'ephemeral' AND parent_id IS NOT NULL AND time_deleted IS NULL;

/* Enforce name-uniqueness of floating (service) IPs at fleet level. */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_floating_ip_by_name on omicron.public.external_ip (
    name
) WHERE
    kind = 'floating' AND
    time_deleted is NULL AND
    project_id is NULL;

/* Enforce name-uniqueness of floating IPs at project level. */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_floating_ip_by_name_and_project on omicron.public.external_ip (
    project_id,
    name
) WHERE
    kind = 'floating' AND
    time_deleted is NULL AND
    project_id is NOT NULL;

CREATE VIEW IF NOT EXISTS omicron.public.floating_ip AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    ip_pool_id,
    ip_pool_range_id,
    is_service,
    parent_id,
    ip,
    project_id
FROM
    omicron.public.external_ip
WHERE
    omicron.public.external_ip.kind = 'floating' AND
    project_id IS NOT NULL;

/*******************************************************************/

/*
 * Sagas
 */

CREATE TYPE IF NOT EXISTS omicron.public.saga_state AS ENUM (
    'running',
    'unwinding',
    'done',
    'abandoned'
);


CREATE TABLE IF NOT EXISTS omicron.public.saga (
    /* immutable fields */

    /* unique identifier for this execution */
    id UUID PRIMARY KEY,
    /* unique id of the creator */
    creator UUID NOT NULL,
    /* time the saga was started */
    time_created TIMESTAMPTZ NOT NULL,
    /* saga name */
    name STRING(128) NOT NULL,
    /* saga DAG (includes params and name) */
    saga_dag JSONB NOT NULL,

    /*
     * TODO:
     * - id for current SEC (maybe NULL?)
     * - time of last adoption
     * - previous SEC? previous adoption time?
     * - number of adoptions?
     */
    saga_state omicron.public.saga_state NOT NULL,
    current_sec UUID,
    adopt_generation INT NOT NULL,
    adopt_time TIMESTAMPTZ NOT NULL
);

/*
 * For recovery (and probably takeover), we need to be able to list running
 * sagas by SEC.  We need to paginate this list by the id.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_saga_by_sec ON omicron.public.saga (
    current_sec, id
) WHERE saga_state != 'done';

/*
 * TODO more indexes for Saga?
 * - Debugging and/or reporting: saga_name? creator?
 */
/*
 * TODO: This is a data-carrying enum, see note on disk_state.
 *
 * See <https://github.com/oxidecomputer/omicron/issues/312>.
 */
-- CREATE TYPE omicron.public.saga_node_event_type AS ENUM (
--    'started',
--    'succeeded',
--    'failed'
--    'undo_started'
--    'undo_finished'
-- );

CREATE TABLE IF NOT EXISTS omicron.public.saga_node_event (
    saga_id UUID NOT NULL,
    node_id INT NOT NULL,
    -- event_type omicron.public.saga_node_event_type NOT NULL,
    event_type STRING(31) NOT NULL,
    data JSONB,
    event_time TIMESTAMPTZ NOT NULL,
    creator UUID NOT NULL,

    /*
     * It's important to be able to list the nodes in a saga.  We put the
     * node_id in the saga so that we can paginate the list.
     *
     * We make it a UNIQUE index and include the event_type to prevent two SECs
     * from attempting to record the same event for the same saga.  Whether this
     * should be allowed is still TBD.
     */
    PRIMARY KEY (saga_id, node_id, event_type)
);

/*******************************************************************/

/*
 * Sessions for use by web console.
 */
CREATE TABLE IF NOT EXISTS omicron.public.console_session (
    id UUID PRIMARY KEY,
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);

-- to be used for cleaning up old tokens
-- It's okay that this index is non-unique because we don't need to page through
-- this list.  We'll just grab the next N, delete them, then repeat.
CREATE INDEX IF NOT EXISTS lookup_console_by_creation
    ON omicron.public.console_session (time_created);

-- This index is used to remove sessions for a user that's being deleted.
CREATE INDEX IF NOT EXISTS lookup_console_by_silo_user
    ON omicron.public.console_session (silo_user_id);

-- We added a UUID as the primary key, but we need the token to keep acting like
-- it did before. "When you change a primary key with ALTER PRIMARY KEY, the old
-- primary key index becomes a secondary index." We chose to use DROP CONSTRAINT
-- and ADD CONSTRAINT instead and manually create the index.
-- https://www.cockroachlabs.com/docs/v22.1/primary-key#changing-primary-key-columns
CREATE UNIQUE INDEX IF NOT EXISTS console_session_token_unique
	ON omicron.public.console_session (token);

/*******************************************************************/

-- Describes a single uploaded TUF repo.
--
-- Identified by both a random uuid and its SHA256 hash. The hash could be the
-- primary key, but it seems unnecessarily large and unwieldy.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,

    -- TODO: Repos fetched over HTTP will not have a SHA256 hash; this is an
    -- implementation detail of our ZIP archives.
    sha256 STRING(64) NOT NULL,

    -- The version of the targets.json role that was used to generate the repo.
    targets_role_version INT NOT NULL,

    -- The valid_until time for the repo.
    -- TODO: Figure out timestamp validity policy for uploaded repos vs those
    -- fetched over HTTP; my (iliana's) current presumption is that we will make
    -- this NULL for uploaded ZIP archives of repos.
    valid_until TIMESTAMPTZ NOT NULL,

    -- The system version described in the TUF repo.
    --
    -- This is the "true" primary key, but is not treated as such in the
    -- database because we may want to change this format in the future.
    -- Re-doing primary keys is annoying.
    --
    -- Because the system version is embedded in the repo's artifacts.json,
    -- each system version is associated with exactly one checksum.
    system_version STRING(64) NOT NULL,

    -- For debugging only:
    -- Filename provided by the user.
    file_name TEXT NOT NULL,

    -- Set when the repository's artifacts can be deleted from replication.
    time_pruned TIMESTAMPTZ,

    CONSTRAINT unique_checksum UNIQUE (sha256),
    CONSTRAINT unique_system_version UNIQUE (system_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS tuf_repo_not_pruned
    ON omicron.public.tuf_repo (id)
    WHERE time_pruned IS NULL;

-- Describes an individual artifact from an uploaded TUF repo.
--
-- In the future, this may also be used to describe artifacts that are fetched
-- from a remote TUF repo, but that requires some additional design work.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    version STRING(64) NOT NULL,
    -- This used to be an enum but is now a string, because it can represent
    -- artifact kinds currently unknown to a particular version of Nexus as
    -- well.
    kind STRING(63) NOT NULL,

    -- The time this artifact was first recorded.
    time_created TIMESTAMPTZ NOT NULL,

    -- The SHA256 hash of the artifact, typically obtained from the TUF
    -- targets.json (and validated at extract time).
    sha256 STRING(64) NOT NULL,
    -- The length of the artifact, in bytes.
    artifact_size INT8 NOT NULL,

    -- The generation number this artifact was added for.
    generation_added INT8 NOT NULL,

    -- Sign (root key hash table) hash of a signed RoT or RoT bootloader image.
    sign BYTES, -- nullable

    -- Board (caboose BORD) for artifacts that are Hubris archives.
    board TEXT, -- nullable (null for non-Hubris artifacts)

    CONSTRAINT unique_name_version_kind UNIQUE (name, version, kind)
);

CREATE UNIQUE INDEX IF NOT EXISTS tuf_artifact_added
    ON omicron.public.tuf_artifact (generation_added, id)
    STORING (name, version, kind, time_created, sha256, artifact_size);

-- RFD 554: (kind, hash) is unique for artifacts. This index is used while
-- looking up artifacts.
CREATE UNIQUE INDEX IF NOT EXISTS tuf_artifact_kind_sha256
    ON omicron.public.tuf_artifact (kind, sha256);

-- Reflects that a particular artifact was provided by a particular TUF repo.
-- This is a many-many mapping.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_artifact (
    tuf_repo_id UUID NOT NULL,
    tuf_artifact_id UUID NOT NULL,

    PRIMARY KEY (tuf_repo_id, tuf_artifact_id)
);

-- Generation number for the current list of TUF artifacts the system wants.
-- This is incremented whenever a TUF repo is added or removed.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_generation (
    -- There should only be one row of this table for the whole DB.
    -- It's a little goofy, but filter on "singleton = true" before querying
    -- or applying updates, and you'll access the singleton row.
    --
    -- We also add a constraint on this table to ensure it's not possible to
    -- access the version of this table with "singleton = false".
    singleton BOOL NOT NULL PRIMARY KEY,
    -- Generation number owned and incremented by Nexus
    generation INT8 NOT NULL,

    CHECK (singleton = true)
);
INSERT INTO omicron.public.tuf_generation (
    singleton,
    generation
) VALUES
    (TRUE, 1)
ON CONFLICT DO NOTHING;

-- Trusted TUF root roles, used to verify TUF repo signatures
CREATE TABLE IF NOT EXISTS omicron.public.tuf_trust_root (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    root_role JSONB NOT NULL
);

-- This index is used for paginating through non-deleted roots.
CREATE UNIQUE INDEX IF NOT EXISTS tuf_trust_root_by_id
ON omicron.public.tuf_trust_root (id)
WHERE
    time_deleted IS NULL;

/*******************************************************************/

-- The source of the software release that should be deployed to the rack.
CREATE TYPE IF NOT EXISTS omicron.public.target_release_source AS ENUM (
    'unspecified',
    'system_version'
);

-- Software releases that should be/have been deployed to the rack. The
-- current target release is the one with the largest generation number.
CREATE TABLE IF NOT EXISTS omicron.public.target_release (
    generation INT8 NOT NULL PRIMARY KEY,
    time_requested TIMESTAMPTZ NOT NULL,
    release_source omicron.public.target_release_source NOT NULL,
    tuf_repo_id UUID, -- "foreign key" into the `tuf_repo` table
    CONSTRAINT tuf_repo_for_system_version CHECK (
      (release_source != 'system_version' AND tuf_repo_id IS NULL) OR
      (release_source = 'system_version' AND tuf_repo_id IS NOT NULL)
    )
);

-- System software is by default from the `install` dataset.
INSERT INTO omicron.public.target_release (
    generation,
    time_requested,
    release_source,
    tuf_repo_id
) VALUES (
    1,
    NOW(),
    'unspecified',
    NULL
) ON CONFLICT DO NOTHING;

/*******************************************************************/

/*
 * Support Bundles
 */


CREATE TYPE IF NOT EXISTS omicron.public.support_bundle_state AS ENUM (
  -- The bundle is currently being created.
  --
  -- It might have storage that is partially allocated on a sled.
  'collecting',

  -- The bundle has been collected successfully, and has storage on
  -- a particular sled.
  'active',

  -- The user has explicitly requested that a bundle be destroyed.
  -- We must ensure that storage backing that bundle is gone before
  -- it is automatically deleted.
  'destroying',

  -- The support bundle is failing.
  -- This happens when Nexus is expunged partway through collection.
  --
  -- A different Nexus must ensure that storage is gone before the
  -- bundle can be marked "failed".
  'failing',

  -- The bundle has finished failing.
  --
  -- The only action that can be taken on this bundle is to delete it.
  'failed'
);

CREATE TABLE IF NOT EXISTS omicron.public.support_bundle (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    reason_for_creation TEXT NOT NULL,
    reason_for_failure TEXT,
    state omicron.public.support_bundle_state NOT NULL,
    zpool_id UUID NOT NULL,
    dataset_id UUID NOT NULL,

    -- The Nexus which is in charge of collecting the support bundle,
    -- and later managing its storage.
    assigned_nexus UUID,

    user_comment TEXT

);

-- The "UNIQUE" part of this index helps enforce that we allow one support bundle
-- per debug dataset. This constraint can be removed, if the query responsible
-- for allocation changes to allocate more intelligently.
CREATE UNIQUE INDEX IF NOT EXISTS one_bundle_per_dataset ON omicron.public.support_bundle (
    dataset_id
);

CREATE INDEX IF NOT EXISTS lookup_bundle_by_nexus ON omicron.public.support_bundle (
    assigned_nexus
);

CREATE INDEX IF NOT EXISTS lookup_bundle_by_creation ON omicron.public.support_bundle (
    time_created
);

/*******************************************************************/

/*
 * DNS Propagation
 *
 * The tables here are the source of truth of DNS data for both internal and
 * external DNS.
 */

/*
 * A DNS group is a collection of DNS zones covered by a single version number.
 * We have two DNS Groups in our system: "internal" (for internal service
 * discovery) and "external" (which we expose on customer networks to provide
 * DNS for our own customer-facing services, like the API and console).
 *
 * Each DNS server is associated with exactly one DNS group.  Nexus propagates
 * the entire contents of a DNS group (i.e., all of its zones and all of those
 * zones' DNS names and associated records) to every server in that group.
 */
CREATE TYPE IF NOT EXISTS omicron.public.dns_group AS ENUM (
    'internal',
    'external'
);

/*
 * A DNS Zone is basically just a DNS name at the root of a subtree served by
 * one of our DNS servers.  In a typical system, there would be two DNS zones:
 *
 * (1) in the "internal" DNS group, a zone called "control-plane.oxide.internal"
 *     used by the control plane for internal service discovery
 *
 * (2) in the "external" DNS group, a zone whose name is owned by the customer
 *     and specified when the rack is set up for the first time.  We will use
 *     this zone to advertise addresses for the services we provide on the
 *     customer network (i.e., the API and console).
 */
CREATE TABLE IF NOT EXISTS omicron.public.dns_zone (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    dns_group omicron.public.dns_group NOT NULL,
    zone_name TEXT NOT NULL
);

/*
 * It's allowed (although probably not correct) for the same DNS zone to appear
 * in both the internal and external groups.  It is not allowed to specify the
 * same DNS zone twice within the same group.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_dns_zone_by_group ON omicron.public.dns_zone (
    dns_group, zone_name
);

/*
 * All the data associated with a DNS group is gathered together and assigned a
 * single version number, sometimes called a generation number.  When changing
 * the DNS data for a group (e.g., to add a new DNS name), clients first insert
 * a new row into this table with the next available generation number.  (This
 * table is not strictly necessary.  Instead, we could put the current version
 * number for the group into a `dns_group` table, and clients could update that
 * instead of inserting into this table.  But by using a table here, we have a
 * debugging record of all past generation updates, including metadata about who
 * created them and why.)
 */
CREATE TABLE IF NOT EXISTS omicron.public.dns_version (
    dns_group omicron.public.dns_group NOT NULL,
    version INT8 NOT NULL,

    /* These fields are for debugging only. */
    time_created TIMESTAMPTZ NOT NULL,
    creator TEXT NOT NULL,
    comment TEXT NOT NULL,

    PRIMARY KEY(dns_group, version)
);

/*
 * The meat of the DNS data: a list of DNS names.  Each name has one or more
 * records stored in JSON.
 *
 * To facilitate clients getting a consistent snapshot of the DNS data at a
 * given version, each name is stored with the version in which it was added and
 * (optionally) the version in which it was removed.  The name and record data
 * are immutable, so changing the records for a given name should be expressed
 * as removing the old name (setting "version_removed") and creating a new
 * record for the same name at a new version.
 */
CREATE TABLE IF NOT EXISTS omicron.public.dns_name (
    dns_zone_id UUID NOT NULL,
    version_added INT8 NOT NULL,
    version_removed INT8,
    name TEXT NOT NULL,
    dns_record_data JSONB NOT NULL,

    PRIMARY KEY (dns_zone_id, name, version_added)
);

/*
 * Any given live name should only exist once.  (Put differently: the primary
 * key already prevents us from having the same name added twice in the same
 * version.  But you should also not be able to add a name in any version if the
 * name is currently still live (i.e., version_removed IS NULL).
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_dns_name_by_zone ON omicron.public.dns_name (
    dns_zone_id, name
) WHERE version_removed IS NULL;

/*******************************************************************/

/*
 * Identity and Access Management (IAM)
 *
 * **For more details and a worked example using the tables here, see the
 * documentation for the omicron_nexus crate, "authz" module.**
 */

/*
 * Users built into the system
 *
 * The ids and names for these users are well-known (i.e., they are used by
 * Nexus directly, so changing these would potentially break compatibility).
 */
CREATE TABLE IF NOT EXISTS omicron.public.user_builtin (
    /*
     * Identity metadata
     *
     * TODO-cleanup This uses the "resource identity" pattern because we want a
     * name and description, but it's not valid to support soft-deleting these
     * records.
     */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_user_builtin_by_name ON omicron.public.user_builtin (name);

/* User used by Nexus to create other users.  Do NOT add more users here! */
INSERT INTO omicron.public.user_builtin (
    id,
    name,
    description,
    time_created,
    time_modified
) VALUES (
    /* NOTE: this uuid and name are duplicated in nexus::authn. */
    '001de000-05e4-4000-8000-000000000001',
    'db-init',
    'user used for database initialization',
    NOW(),
    NOW()
) ON CONFLICT DO NOTHING;

/*
 * OAuth 2.0 Device Authorization Grant (RFC 8628)
 */

-- Device authorization requests. These records are short-lived,
-- and removed as soon as a token is granted. This allows us to
-- use the `user_code` as primary key, despite it not having very
-- much entropy.
-- TODO: A background task should remove unused expired records.
CREATE TABLE IF NOT EXISTS omicron.public.device_auth_request (
    user_code STRING(20) PRIMARY KEY,
    client_id UUID NOT NULL,
    device_code STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ NOT NULL,
    -- requested TTL for the token in seconds (if specified by the user)
    token_ttl_seconds INT8 CHECK (token_ttl_seconds > 0)
);

-- Access tokens granted in response to successful device authorization flows.
CREATE TABLE IF NOT EXISTS omicron.public.device_access_token (
    id UUID PRIMARY KEY,
    token STRING(40) NOT NULL,
    client_id UUID NOT NULL,
    device_code STRING(40) NOT NULL,
    silo_user_id UUID NOT NULL,
    time_requested TIMESTAMPTZ NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ
);

-- This UNIQUE constraint is critical for ensuring that at most
-- one token is ever created for a given device authorization flow.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_device_access_token_by_client
    ON omicron.public.device_access_token (client_id, device_code);

-- We added a UUID as the primary key, but we need the token to keep acting like
-- it did before
CREATE UNIQUE INDEX IF NOT EXISTS device_access_token_unique
    ON omicron.public.device_access_token (token);

-- This index is used to remove tokens for a user that's being deleted.
CREATE INDEX IF NOT EXISTS lookup_device_access_token_by_silo_user
    ON omicron.public.device_access_token (silo_user_id);


/*
 * Assignments between users, roles, and resources
 *
 * An actor has a role on a resource if there's a record in this table that
 * points to that actor, role, and resource.
 *
 * For more details and a worked example, see the omicron_nexus::authz
 * module-level documentation.
 */

CREATE TYPE IF NOT EXISTS omicron.public.identity_type AS ENUM (
  'user_builtin',
  'silo_user',
  'silo_group'
);

CREATE TABLE IF NOT EXISTS omicron.public.role_assignment (
    resource_type STRING(63) NOT NULL,
    role_name STRING(63) NOT NULL,

    /*
     * Foreign key into some other resource table.  Which table?  This is
     * identified implicitly by "resource_type" above.
     */
    resource_id UUID NOT NULL,

    /*
     * Foreign key into some other user table.  Which table?  That's determined
     * by "identity_type".
     */
    identity_id UUID NOT NULL,
    identity_type omicron.public.identity_type NOT NULL,

    /*
     * The resource_id, identity_id, and role_name uniquely identify the role
     * assignment.  We include the resource_type and identity_type as
     * belt-and-suspenders, but there should only be one resource type for any
     * resource id and one identity type for any identity id.
     *
     * By organizing the primary key by resource id, then role name, then
     * identity information, we can use it to generated paginated listings of
     * role assignments for a resource, ordered by role name.  It's surprisingly
     * load-bearing that "identity_type" appears last.  That's because when we
     * list a page of role assignments for a resource sorted by role name and
     * then identity id, every field _except_ identity_type is used in the
     * query's filter or sort order.  If identity_type appeared before one of
     * those fields, CockroachDB wouldn't necessarily know it could use the
     * primary key index to efficiently serve the query.
     */
    PRIMARY KEY(
        resource_id,
        resource_type,
        role_name,
        identity_id,
        identity_type
     )
);

/*******************************************************************/

/*
 * External Networking
 *
 * **For more details on external networking see RFD 267**
 */

CREATE TYPE IF NOT EXISTS omicron.public.address_lot_kind AS ENUM (
    'infra',
    'pool'
);

CREATE TABLE IF NOT EXISTS omicron.public.address_lot (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    kind omicron.public.address_lot_kind NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_address_lot_by_name ON omicron.public.address_lot (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.address_lot_block (
    id UUID PRIMARY KEY,
    address_lot_id UUID NOT NULL,
    first_address INET NOT NULL,
    last_address INET NOT NULL
);

CREATE INDEX IF NOT EXISTS lookup_address_lot_block_by_lot ON omicron.public.address_lot_block (
    address_lot_id
);

CREATE TABLE IF NOT EXISTS omicron.public.address_lot_rsvd_block (
    id UUID PRIMARY KEY,
    address_lot_id UUID NOT NULL,
    first_address INET NOT NULL,
    last_address INET NOT NULL,
    anycast BOOL NOT NULL
);

CREATE INDEX IF NOT EXISTS lookup_address_lot_rsvd_block_by_lot ON omicron.public.address_lot_rsvd_block (
    address_lot_id
);

CREATE INDEX IF NOT EXISTS lookup_address_lot_rsvd_block_by_anycast ON omicron.public.address_lot_rsvd_block (
    anycast
);

CREATE TABLE IF NOT EXISTS omicron.public.loopback_address (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    address_lot_block_id UUID NOT NULL,
    rsvd_address_lot_block_id UUID NOT NULL,
    rack_id UUID NOT NULL,
    switch_location TEXT NOT NULL,
    address INET NOT NULL,
    anycast BOOL NOT NULL
);

/* TODO https://github.com/oxidecomputer/omicron/issues/3001 */

CREATE UNIQUE INDEX IF NOT EXISTS lookup_loopback_address ON omicron.public.loopback_address (
    address, rack_id, switch_location
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port (
    id UUID PRIMARY KEY,
    rack_id UUID,
    switch_location TEXT,
    port_name TEXT,
    port_settings_id UUID,

    CONSTRAINT switch_port_rack_locaction_name_unique UNIQUE (
        rack_id, switch_location, port_name
    )
);

CREATE INDEX IF NOT EXISTS lookup_switch_port_by_port_settings ON omicron.public.switch_port (port_settings_id);

/* port settings groups included from port settings objects */
CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_groups (
    port_settings_id UUID,
    port_settings_group_id UUID,

    PRIMARY KEY (port_settings_id, port_settings_group_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_group (
    id UUID PRIMARY KEY,
    /* port settings in this group */
    port_settings_id UUID NOT NULL,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_switch_port_settings_group_by_name ON omicron.public.switch_port_settings_group (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS switch_port_settings_by_name ON omicron.public.switch_port_settings (
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.switch_port_geometry AS ENUM (
    'Qsfp28x1',
    'Qsfp28x2',
    'Sfp28x4'
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_port_config (
    port_settings_id UUID PRIMARY KEY,
    geometry omicron.public.switch_port_geometry
);

CREATE TYPE IF NOT EXISTS omicron.public.switch_link_fec AS ENUM (
    'Firecode',
    'None',
    'Rs'
);

CREATE TYPE IF NOT EXISTS omicron.public.switch_link_speed AS ENUM (
    '0G',
    '1G',
    '10G',
    '25G',
    '40G',
    '50G',
    '100G',
    '200G',
    '400G'
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_link_config (
    port_settings_id UUID,
    link_name TEXT,
    mtu INT4,
    fec omicron.public.switch_link_fec,
    speed omicron.public.switch_link_speed,
    autoneg BOOL NOT NULL DEFAULT false,
    lldp_link_config_id UUID,
    tx_eq_config_id UUID,

    PRIMARY KEY (port_settings_id, link_name)
);

CREATE TABLE IF NOT EXISTS omicron.public.lldp_link_config (
    id UUID PRIMARY KEY,
    enabled BOOL NOT NULL,
    link_name STRING(63),
    link_description STRING(512),
    chassis_id STRING(63),
    system_name STRING(63),
    system_description STRING(612),
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    management_ip INET
);

CREATE TABLE IF NOT EXISTS omicron.public.tx_eq_config (
    id UUID PRIMARY KEY,
    pre1 INT4,
    pre2 INT4,
    main INT4,
    post2 INT4,
    post1 INT4
);

CREATE TYPE IF NOT EXISTS omicron.public.switch_interface_kind AS ENUM (
    'primary',
    'vlan',
    'loopback'
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_interface_config (
    port_settings_id UUID,
    id UUID PRIMARY KEY,
    interface_name TEXT NOT NULL,
    v6_enabled BOOL NOT NULL,
    kind omicron.public.switch_interface_kind
);

CREATE UNIQUE INDEX IF NOT EXISTS switch_port_settings_interface_config_by_id ON omicron.public.switch_port_settings_interface_config (
    port_settings_id, interface_name
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_vlan_interface_config (
    interface_config_id UUID,
    vid INT4,

    PRIMARY KEY (interface_config_id, vid)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_route_config (
    port_settings_id UUID,
    interface_name TEXT,
    dst INET,
    gw INET,
    vid INT4,
    rib_priority INT2,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, interface_name, dst, gw)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config (
    port_settings_id UUID,
    bgp_config_id UUID NOT NULL,
    interface_name TEXT,
    addr INET,
    hold_time INT8,
    idle_hold_time INT8,
    delay_open INT8,
    connect_retry INT8,
    keepalive INT8,
    remote_asn INT8,
    min_ttl INT2,
    md5_auth_key TEXT,
    multi_exit_discriminator INT8,
    local_pref INT8,
    enforce_first_as BOOLEAN NOT NULL DEFAULT false,
    allow_import_list_active BOOLEAN NOT NULL DEFAULT false,
    allow_export_list_active BOOLEAN NOT NULL DEFAULT false,
    vlan_id INT4,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, interface_name, addr)
);

CREATE INDEX IF NOT EXISTS lookup_sps_bgp_peer_config_by_bgp_config_id on omicron.public.switch_port_settings_bgp_peer_config(
    bgp_config_id
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config_communities (
    port_settings_id UUID NOT NULL,
    interface_name TEXT NOT NULL,
    addr INET NOT NULL,
    community INT8 NOT NULL,

    PRIMARY KEY (port_settings_id, interface_name, addr, community)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config_allow_import (
    port_settings_id UUID NOT NULL,
    interface_name TEXT NOT NULL,
    addr INET NOT NULL,
    prefix INET NOT NULL,

    PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config_allow_export (
    port_settings_id UUID NOT NULL,
    interface_name TEXT NOT NULL,
    addr INET NOT NULL,
    prefix INET NOT NULL,

    PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
);

CREATE TABLE IF NOT EXISTS omicron.public.bgp_config (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    asn INT8 NOT NULL,
    vrf TEXT,
    bgp_announce_set_id UUID NOT NULL,
    shaper TEXT,
    checker TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_bgp_config_by_name ON omicron.public.bgp_config (
    name
) WHERE
    time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS lookup_bgp_config_by_asn ON omicron.public.bgp_config (
    asn
) WHERE time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.bgp_announce_set (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_bgp_announce_set_by_name ON omicron.public.bgp_announce_set (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.bgp_announcement (
    announce_set_id UUID,
    address_lot_block_id UUID NOT NULL,
    network INET,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (announce_set_id, network)
);

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_address_config (
    port_settings_id UUID,
    address_lot_block_id UUID NOT NULL,
    rsvd_address_lot_block_id UUID NOT NULL,
    address INET,
    interface_name TEXT,
    vlan_id INT4,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, address, interface_name)
);

CREATE TABLE IF NOT EXISTS omicron.public.bootstore_keys (
    key TEXT NOT NULL PRIMARY KEY,
    generation INT8 NOT NULL
);

/*
 * Hardware/software inventory
 *
 * See RFD 433 for details.  Here are the highlights.
 *
 * Omicron periodically collects hardware/software inventory data from the
 * running system and stores it into the database.  Each discrete set of data is
 * called a **collection**.  Each collection contains lots of different kinds of
 * data, so there are many tables here.  For clarity, these tables are prefixed
 * with:
 *
 *     `inv_*` (examples: `inv_collection`, `inv_service_processor`)
 *
 *         Describes the complete set of hardware and software in the system.
 *         Rows in these tables are immutable, but they describe mutable facts
 *         about hardware and software (e.g., the slot that a disk is in).  When
 *         these facts change (e.g., a disk moves between slots), a new set of
 *         records is written.
 *
 * All rows in the `inv_*` tables point back to a particular collection.  They
 * represent the state observed at some particular time.  Generally, if two
 * observations came from two different places, they're not put into the same
 * row of the same table.  For example, caboose information comes from the SP,
 * but it doesn't go into the `inv_service_processor` table.  It goes in a
 * separate `inv_caboose` table.  This is debatable but it preserves a clearer
 * record of exactly what information came from where, since the separate record
 * has its own "source" and "time_collected".
 *
 * Information about service processors and roots of trust are joined with
 * information reported by sled agents via the baseboard id.
 *
 * Hardware and software identifiers are normalized for the usual database
 * design reasons.  This means instead of storing hardware and software
 * identifiers directly in the `inv_*` tables, these tables instead store
 * foreign keys into one of these groups of tables, whose names are also
 * prefixed for clarity:
 *
 *     `hw_*` (example: `hw_baseboard_id`)
 *
 *         Maps hardware-provided identifiers to UUIDs that are used as foreign
 *         keys in the rest of the schema. (Avoids embedding these identifiers
 *         into all the other tables.)
 *
 *     `sw_*` (example: `sw_caboose`)
 *
 *         Maps software-provided identifiers to UUIDs that are used as foreign
 *         keys in the rest of the schema. (Avoids embedding these identifiers
 *         into all the other tables.)
 *
 * Records in these tables are shared across potentially many collections.  To
 * see why this is useful, consider that `sw_caboose` records contain several
 * long identifiers (e.g., git commit, SHA sums) and in practice, most of the
 * time, we expect that all components of a given type will have the exact same
 * cabooses.  Rather than store the caboose contents in each
 * `inv_service_processor` row (for example), often replicating the exact same
 * contents for each SP for each collection, these rows just have pointers into
 * the `sw_caboose` table that stores this data once.  (This also makes it much
 * easier to determine that these components _do_ have the same cabooses.)
 *
 * On PC systems (i.e., non-Oxide hardware), most of these tables will be empty
 * because we do not support hardware inventory on these systems.
 *
 * Again, see RFD 433 for more on all this.
 */

/*
 * baseboard ids: this table assigns uuids to distinct part/serial values
 *
 * Usually we include the baseboard revision number when we reference the part
 * number and serial number.  The revision number is deliberately left out here.
 * If we happened to see the same baseboard part number and serial number with
 * different revisions, that's the same baseboard.
 */
CREATE TABLE IF NOT EXISTS omicron.public.hw_baseboard_id (
    id UUID PRIMARY KEY,
    part_number TEXT NOT NULL,
    serial_number TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS lookup_baseboard_id_by_props
    ON omicron.public.hw_baseboard_id (part_number, serial_number);

/* power states reportable by the SP */
CREATE TYPE IF NOT EXISTS omicron.public.hw_power_state AS ENUM (
    'A0',
    'A1',
    'A2'
);

/* root of trust firmware slots */
CREATE TYPE IF NOT EXISTS omicron.public.hw_rot_slot AS ENUM (
    'A',
    'B'
);

/* cabooses: this table assigns unique ids to distinct caboose contents */
CREATE TABLE IF NOT EXISTS omicron.public.sw_caboose (
    id UUID PRIMARY KEY,
    board TEXT NOT NULL,
    git_commit TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    sign TEXT -- nullable
);

/*
 * We use a complete and a partial index to ensure uniqueness.
 * This is necessary because the sign column is NULLable, but in SQL, NULL values
 * are considered distinct. That means that a single complete index on all of these
 * columns would allow duplicate rows where sign is NULL, which we don't want.
 */
CREATE UNIQUE INDEX IF NOT EXISTS caboose_properties
    on omicron.public.sw_caboose (board, git_commit, name, version, sign);

CREATE UNIQUE INDEX IF NOT EXISTS caboose_properties_no_sign
    on omicron.public.sw_caboose (board, git_commit, name, version)
    WHERE sign IS NULL;

/* root of trust pages: this table assigns unique ids to distinct RoT CMPA
   and CFPA page contents, each of which is a 512-byte blob */
CREATE TABLE IF NOT EXISTS omicron.public.sw_root_of_trust_page (
    id UUID PRIMARY KEY,
    data_base64 TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS root_of_trust_page_properties
    on omicron.public.sw_root_of_trust_page (data_base64);

/* Inventory Collections */

-- list of all collections
CREATE TABLE IF NOT EXISTS omicron.public.inv_collection (
    id UUID PRIMARY KEY,
    time_started TIMESTAMPTZ NOT NULL,
    time_done TIMESTAMPTZ NOT NULL,
    collector TEXT NOT NULL
);
-- Supports finding latest collection (to use) or the oldest collection (to
-- clean up)
CREATE INDEX IF NOT EXISTS inv_collection_by_time_started
    ON omicron.public.inv_collection (time_started);

CREATE INDEX IF NOT EXISTS inv_collectionby_time_done
    ON omicron.public.inv_collection (time_done DESC);

-- list of errors generated during a collection
CREATE TABLE IF NOT EXISTS omicron.public.inv_collection_error (
    inv_collection_id UUID NOT NULL,
    idx INT4 NOT NULL,
    message TEXT
);
CREATE INDEX IF NOT EXISTS errors_by_collection
    ON omicron.public.inv_collection_error (inv_collection_id, idx);

/* what kind of slot MGS reported a device in */
CREATE TYPE IF NOT EXISTS omicron.public.sp_type AS ENUM (
    'sled',
    'switch',
    'power'
);

-- observations from and about service processors
-- also see `inv_root_of_trust`
CREATE TABLE IF NOT EXISTS omicron.public.inv_service_processor (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    -- identity of this device according to MGS
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,

    -- Data from MGS "Get SP Info" API.  See MGS API documentation.
    baseboard_revision INT8 NOT NULL,
    hubris_archive_id TEXT NOT NULL,
    power_state omicron.public.hw_power_state NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);

CREATE TYPE IF NOT EXISTS omicron.public.rot_image_error AS ENUM (
        'unchecked',
        'first_page_erased',
        'partially_programmed',
        'invalid_length',
        'header_not_programmed',
        'bootloader_too_small',
        'bad_magic',
        'header_image_size',
        'unaligned_length',
        'unsupported_type',
        'not_thumb2',
        'reset_vector',
        'signature'
);

-- root of trust information reported by SP
-- There's usually one row here for each row in inv_service_processor, but not
-- necessarily.
CREATE TABLE IF NOT EXISTS omicron.public.inv_root_of_trust (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    slot_active omicron.public.hw_rot_slot NOT NULL,
    slot_boot_pref_transient omicron.public.hw_rot_slot, -- nullable
    slot_boot_pref_persistent omicron.public.hw_rot_slot NOT NULL,
    slot_boot_pref_persistent_pending omicron.public.hw_rot_slot, -- nullable
    slot_a_sha3_256 TEXT, -- nullable
    slot_b_sha3_256 TEXT, -- nullable
    stage0_fwid TEXT, -- nullable
    stage0next_fwid TEXT, -- nullable

    slot_a_error omicron.public.rot_image_error, -- nullable
    slot_b_error omicron.public.rot_image_error, -- nullable
    stage0_error omicron.public.rot_image_error, -- nullable
    stage0next_error omicron.public.rot_image_error, -- nullable

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);

-- host phase 1 slots
CREATE TYPE IF NOT EXISTS omicron.public.hw_m2_slot AS ENUM (
    'A',
    'B'
);

-- host phase 1 active slots found
CREATE TABLE IF NOT EXISTS omicron.public.inv_host_phase_1_active_slot (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    -- active phase 1 slot
    slot omicron.public.hw_m2_slot NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);

-- host phase 1 flash hashes found
-- There are usually two rows here for each row in inv_service_processor, but
-- not necessarily (either or both slots' hash collection may fail).
CREATE TABLE IF NOT EXISTS omicron.public.inv_host_phase_1_flash_hash (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    -- phase 1 slot for this hash
    slot omicron.public.hw_m2_slot NOT NULL,
    -- the actual hash of the contents
    hash STRING(64) NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id, slot)
);

CREATE TYPE IF NOT EXISTS omicron.public.caboose_which AS ENUM (
    'sp_slot_0',
    'sp_slot_1',
    'rot_slot_A',
    'rot_slot_B',
    'stage0',
    'stage0next'
);

-- cabooses found
CREATE TABLE IF NOT EXISTS omicron.public.inv_caboose (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    which omicron.public.caboose_which NOT NULL,
    sw_caboose_id UUID NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id, which)
);

CREATE TYPE IF NOT EXISTS omicron.public.root_of_trust_page_which AS ENUM (
    'cmpa',
    'cfpa_active',
    'cfpa_inactive',
    'cfpa_scratch'
);

-- root of trust key signing pages found
CREATE TABLE IF NOT EXISTS omicron.public.inv_root_of_trust_page (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    which omicron.public.root_of_trust_page_which NOT NULL,
    sw_root_of_trust_page_id UUID NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id, which)
);

CREATE TYPE IF NOT EXISTS omicron.public.sled_role AS ENUM (
    -- this sled is directly attached to a Sidecar
    'scrimlet',
    -- everything else
    'gimlet'
);

CREATE TYPE IF NOT EXISTS omicron.public.inv_config_reconciler_status_kind
AS ENUM (
    'not-yet-run',
    'running',
    'idle'
);

CREATE TYPE IF NOT EXISTS omicron.public.inv_zone_manifest_source AS ENUM (
    'installinator',
    'sled-agent'
);

-- observations from and about sled agents
CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_agent (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- URL of the sled agent that reported this data
    source TEXT NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- which system this sled agent reports it's running on
    -- (foreign key into `hw_baseboard_id` table)
    -- This is optional because dev/test systems support running on non-Oxide
    -- hardware.
    hw_baseboard_id UUID,

    -- Many of the following properties are duplicated from the `sled` table,
    -- which predates the current inventory system.
    sled_agent_ip INET NOT NULL,
    sled_agent_port INT4 NOT NULL,
    sled_role omicron.public.sled_role NOT NULL,
    usable_hardware_threads INT8
        CHECK (usable_hardware_threads BETWEEN 0 AND 4294967295) NOT NULL,
    usable_physical_ram INT8 NOT NULL,
    reservoir_size INT8 CHECK (reservoir_size < usable_physical_ram) NOT NULL,

    -- Currently-ledgered `OmicronSledConfig`
    -- (foreign key into `inv_omicron_sled_config` table)
    -- This is optional because newly-added sleds don't yet have a config.
    ledgered_sled_config UUID,

    -- Columns making up the status of the config reconciler.
    reconciler_status_kind omicron.public.inv_config_reconciler_status_kind NOT NULL,
    -- (foreign key into `inv_omicron_sled_config` table)
    -- only present if `reconciler_status_kind = 'running'`
    reconciler_status_sled_config UUID,
    -- only present if `reconciler_status_kind != 'not-yet-run'`
    reconciler_status_timestamp TIMESTAMPTZ,
    -- only present if `reconciler_status_kind != 'not-yet-run'`
    reconciler_status_duration_secs FLOAT,

    -- Columns making up the zone image resolver's zone manifest description:
    --
    -- The path to the boot disk image file.
    zone_manifest_boot_disk_path TEXT NOT NULL,
    -- The source of the zone manifest on the boot disk: from installinator or
    -- sled-agent (synthetic). NULL means there is an error reading the zone manifest.
    zone_manifest_source omicron.public.inv_zone_manifest_source,
    -- The mupdate ID that created the zone manifest if this is from installinator. If
    -- this is NULL, then either the zone manifest is synthetic or there was an
    -- error reading the zone manifest.
    zone_manifest_mupdate_id UUID,
    -- Message describing the status of the zone manifest on the boot disk. If
    -- this is NULL, then the zone manifest was successfully read, and the
    -- inv_zone_manifest_zone table has entries corresponding to the zone
    -- manifest.
    zone_manifest_boot_disk_error TEXT,

    -- Columns making up the zone image resolver's mupdate override description.
    mupdate_override_boot_disk_path TEXT NOT NULL,
    -- The ID of the mupdate override. NULL means either that the mupdate
    -- override was not found or that we failed to read it -- the two cases are
    -- differentiated by the presence of a non-NULL value in the
    -- mupdate_override_boot_disk_error column.
    mupdate_override_id UUID,
    -- Error reading the mupdate override, if any. If this is NULL then
    -- the mupdate override was either successfully read or is not
    -- present.
    mupdate_override_boot_disk_error TEXT,

    -- The sled's CPU family. This is also duplicated with the `sled` table,
    -- similar to `usable_hardware_threads` and friends above.
    cpu_family omicron.public.sled_cpu_family NOT NULL,

    CONSTRAINT reconciler_status_sled_config_present_if_running CHECK (
        (reconciler_status_kind = 'running'
            AND reconciler_status_sled_config IS NOT NULL)
        OR
        (reconciler_status_kind != 'running'
            AND reconciler_status_sled_config IS NULL)
    ),
    CONSTRAINT reconciler_status_timing_present_unless_not_yet_run CHECK (
        (reconciler_status_kind = 'not-yet-run'
            AND reconciler_status_timestamp IS NULL
            AND reconciler_status_duration_secs IS NULL)
        OR
        (reconciler_status_kind != 'not-yet-run'
            AND reconciler_status_timestamp IS NOT NULL
            AND reconciler_status_duration_secs IS NOT NULL)
    ),

    -- For the zone manifest, there are three valid states:
    -- 1. Successfully read from installinator (has mupdate_id, no error)
    -- 2. Synthetic from sled-agent (no mupdate_id, no error)
    -- 3. Error reading (no mupdate_id, has error)
    --
    -- This is equivalent to Result<OmicronZoneManifestSource, String>.
    CONSTRAINT zone_manifest_consistency CHECK (
        (zone_manifest_source = 'installinator'
            AND zone_manifest_mupdate_id IS NOT NULL
            AND zone_manifest_boot_disk_error IS NULL)
        OR (zone_manifest_source = 'sled-agent'
            AND zone_manifest_mupdate_id IS NULL
            AND zone_manifest_boot_disk_error IS NULL)
        OR (
            zone_manifest_source IS NULL
            AND zone_manifest_mupdate_id IS NULL
            AND zone_manifest_boot_disk_error IS NOT NULL
        )
    ),

    -- For the mupdate override, three states are valid:
    -- 1. No override, no error
    -- 2. Override, no error
    -- 3. No override, error
    --
    -- This is equivalent to Result<Option<T>, String>.
    CONSTRAINT mupdate_override_consistency CHECK (
        (mupdate_override_id IS NULL
            AND mupdate_override_boot_disk_error IS NOT NULL)
        OR mupdate_override_boot_disk_error IS NULL
    ),

    PRIMARY KEY (inv_collection_id, sled_id)
);

-- This type name starts with "clear_" for legacy reasons. Prefer "remove" in
-- the future.
CREATE TYPE IF NOT EXISTS omicron.public.clear_mupdate_override_boot_success
AS ENUM (
    'cleared',
    'no-override'
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_config_reconciler (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about);
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,

    -- Most-recently-reconciled `OmicronSledConfig`
    -- (foreign key into `inv_omicron_sled_config` table)
    last_reconciled_config UUID NOT NULL,

    -- Which internal disk slot did we use at boot?
    --
    -- If not NULL, `boot_disk_slot` must be 0 or 1 (corresponding to M2Slot::A
    -- and M2Slot::B, respectively). The column pair `boot_disk_slot` /
    -- `boot_disk_error` represents a Rust `Result`; one or the other must be
    -- non-NULL, but not both.
    boot_disk_slot INT2 CHECK (boot_disk_slot >= 0 AND boot_disk_slot <= 1),
    boot_disk_error TEXT,
    CONSTRAINT boot_disk_slot_or_error CHECK (
        (boot_disk_slot IS NULL AND boot_disk_error IS NOT NULL)
        OR
        (boot_disk_slot IS NOT NULL AND boot_disk_error IS NULL)
    ),

    -- If either error string is present, there was an error reading the boot
    -- partition for the corresponding M2Slot.
    --
    -- For either or both columns if NULL, there will be a row in
    -- `inv_sled_boot_partition` describing the contents of the boot partition
    -- for the given slot. As above 0=a and 1=b.
    boot_partition_a_error TEXT,
    boot_partition_b_error TEXT,

    -- The names below start with "clear_" for legacy reasons. Prefer "remove"
    -- in the future.
    --
    -- Success removing the mupdate override.
    clear_mupdate_override_boot_success omicron.public.clear_mupdate_override_boot_success,
    -- Error removing the mupdate override.
    clear_mupdate_override_boot_error TEXT,

    -- A message describing the result removing the mupdate override on the
    -- non-boot disk (success or error).
    clear_mupdate_override_non_boot_message TEXT,

    -- Three cases:
    --
    -- 1. No remove_mupdate_override instruction was passed in. All three
    --    columns are NULL.
    -- 2. Removing the override was successful. boot_success is NOT NULL,
    --    boot_error is NULL, and non_boot_message is NOT NULL.
    -- 3. Removing the override failed. boot_success is NULL, boot_error is
    --    NOT NULL, and non_boot_message is NOT NULL.
    CONSTRAINT clear_mupdate_override_consistency CHECK (
        (clear_mupdate_override_boot_success IS NULL
         AND clear_mupdate_override_boot_error IS NULL
         AND clear_mupdate_override_non_boot_message IS NULL)
    OR
        (clear_mupdate_override_boot_success IS NOT NULL
         AND clear_mupdate_override_boot_error IS NULL
         AND clear_mupdate_override_non_boot_message IS NOT NULL)
    OR
        (clear_mupdate_override_boot_success IS NULL
         AND clear_mupdate_override_boot_error IS NOT NULL
         AND clear_mupdate_override_non_boot_message IS NOT NULL)
    ),

    PRIMARY KEY (inv_collection_id, sled_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_boot_partition (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- the boot disk slot (0=M2Slot::A, 1=M2Slot::B)
    boot_disk_slot INT2
        CHECK (boot_disk_slot >= 0 AND boot_disk_slot <= 1) NOT NULL,

    -- SHA256 hash of the artifact; if we have a TUF repo containing this OS
    -- image, this will match the artifact hash of the phase 2 image
    artifact_hash STRING(64) NOT NULL,
    -- The length of the artifact in bytes
    artifact_size INT8 NOT NULL,

    -- Fields comprising the header of the phase 2 image
    header_flags INT8 NOT NULL,
    header_data_size INT8 NOT NULL,
    header_image_size INT8 NOT NULL,
    header_target_size INT8 NOT NULL,
    header_sha256 STRING(64) NOT NULL,
    header_image_name TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, boot_disk_slot)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_physical_disk (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,
    -- The slot where this disk was last observed
    slot INT8 CHECK (slot >= 0) NOT NULL,

    vendor STRING(63) NOT NULL,
    model STRING(63) NOT NULL,
    serial STRING(63) NOT NULL,

    variant omicron.public.physical_disk_kind NOT NULL,

    -- PK consisting of:
    -- - Which collection this was
    -- - The sled reporting the disk
    -- - The slot in which this disk was found
    PRIMARY KEY (inv_collection_id, sled_id, slot)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_nvme_disk_firmware (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,
    -- The slot where this disk was last observed
    slot INT8 CHECK (slot >= 0) NOT NULL,

    -- total number of firmware slots the device has
    number_of_slots INT2 CHECK (number_of_slots BETWEEN 1 AND 7) NOT NULL,
    active_slot INT2 CHECK (active_slot BETWEEN 1 AND 7) NOT NULL,
    -- staged firmware slot to be active on reset
    next_active_slot INT2 CHECK (next_active_slot BETWEEN 1 AND 7),
    -- slot1 is distinct in the NVMe spec in the sense that it can be read only
    slot1_is_read_only BOOLEAN,
    -- the firmware version string for each NVMe slot (0 indexed), a NULL means the
    -- slot exists but is empty
    slot_firmware_versions STRING(8)[] CHECK (array_length(slot_firmware_versions, 1) BETWEEN 1 AND 7),

    -- PK consisting of:
    -- - Which collection this was
    -- - The sled reporting the disk
    -- - The slot in which the disk was found
    PRIMARY KEY (inv_collection_id, sled_id, slot)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_zpool (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,

    -- The control plane ID of the zpool
    id UUID NOT NULL,
    sled_id UUID NOT NULL,
    total_size INT NOT NULL,

    -- PK consisting of:
    -- - Which collection this was
    -- - The sled reporting the disk
    -- - The slot in which this disk was found
    PRIMARY KEY (inv_collection_id, sled_id, id)
);

-- Allow looking up the most recent Zpool by ID
CREATE INDEX IF NOT EXISTS inv_zpool_by_id_and_time ON omicron.public.inv_zpool (id, time_collected DESC);

CREATE TABLE IF NOT EXISTS omicron.public.inv_dataset (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,

    -- The control plane ID of the dataset.
    -- This is nullable because datasets have been historically
    -- self-managed by the Sled Agent, and some don't have explicit UUIDs.
    id UUID,

    name TEXT NOT NULL,
    available INT8 NOT NULL,
    used INT8 NOT NULL,
    quota INT8,
    reservation INT8,
    compression TEXT NOT NULL,

    -- PK consisting of:
    -- - Which collection this was
    -- - The sled reporting the disk
    -- - The name of this dataset
    PRIMARY KEY (inv_collection_id, sled_id, name)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- ID of this sled config. A given inventory report from a sled agent may
    -- contain 0-3 sled configs, so we generate these IDs on insertion and
    -- record them as the foreign keys in `inv_sled_agent`.
    id UUID NOT NULL,

    -- config generation
    generation INT8 NOT NULL,

    -- remove mupdate override ID, if set
    remove_mupdate_override UUID,

    -- desired artifact hash for internal disk slots' boot partitions
    -- NULL is translated to `HostPhase2DesiredContents::CurrentContents`
    host_phase_2_desired_slot_a STRING(64),
    host_phase_2_desired_slot_b STRING(64),

    PRIMARY KEY (inv_collection_id, id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_disk_result (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique id for this physical disk
    disk_id UUID NOT NULL,

    -- error message; if NULL, an "ok" result
    error_message TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, disk_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_dataset_result (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique id for this dataset
    dataset_id UUID NOT NULL,

    -- error message; if NULL, an "ok" result
    error_message TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_orphaned_dataset (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- These three columns compose a `DatasetName`. Other tables that store a
    -- `DatasetName` use a nullable `zone_name` (since it's only supposed to be
    -- set for datasets with `kind = 'zone'`). This table instead uses the empty
    -- string for non-'zone' kinds, which allows the column to be NOT NULL and
    -- hence be a member of our primary key. (We have no other unique ID to
    -- distinguish different `DatasetName`s.)
    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    zone_name TEXT NOT NULL,
    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone' AND zone_name = '') OR
      (kind = 'zone' AND zone_name != '')
    ),

    reason TEXT NOT NULL,

    -- The control plane ID of the dataset.
    -- This is nullable because this is attached as the `oxide:uuid` property in
    -- ZFS, and we can't guarantee it exists for any given dataset.
    id UUID,

    -- Properties of the dataset at the time we detected it was an orphan.
    mounted BOOL NOT NULL,
    available INT8 NOT NULL,
    used INT8 NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, pool_id, kind, zone_name)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_zone_result (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique id for this zone
    zone_id UUID NOT NULL,

    -- error message; if NULL, an "ok" result
    error_message TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, zone_id)
);

-- A table describing a single zone within a zone manifest collected by inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_zone_manifest_zone (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- Zone file name, part of the primary key within this table.
    zone_file_name TEXT NOT NULL,

    -- The full path to the file.
    path TEXT NOT NULL,

    -- The expected file size.
    expected_size INT8 NOT NULL,

    -- The expected hash.
    expected_sha256 STRING(64) NOT NULL,

    -- The error while reading the zone or matching it to the manifest, if any.
    -- NULL indicates success.
    error TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, zone_file_name)
);

-- A table describing status for a single zone manifest on a non-boot disk
-- collected by inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_zone_manifest_non_boot (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique ID for this non-boot disk
    non_boot_zpool_id UUID NOT NULL,

    -- The full path to the zone manifest.
    path TEXT NOT NULL,

    -- Whether the non-boot disk is in a valid state.
    is_valid BOOLEAN NOT NULL,

    -- A message attached to this disk.
    message TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, non_boot_zpool_id)
);

-- A table describing status for a single mupdate override on a non-boot disk
-- collected by inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_mupdate_override_non_boot (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique id for this non-boot disk
    non_boot_zpool_id UUID NOT NULL,

    -- The full path to the mupdate override file.
    path TEXT NOT NULL,

    -- Whether the non-boot disk is in a valid state.
    is_valid BOOLEAN NOT NULL,

    -- A message attached to this disk.
    message TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, non_boot_zpool_id)
);

CREATE TYPE IF NOT EXISTS omicron.public.zone_type AS ENUM (
  'boundary_ntp',
  'clickhouse',
  'clickhouse_keeper',
  'clickhouse_server',
  'cockroach_db',
  'crucible',
  'crucible_pantry',
  'external_dns',
  'internal_dns',
  'internal_ntp',
  'nexus',
  'oximeter'
);

CREATE TYPE IF NOT EXISTS omicron.public.inv_zone_image_source AS ENUM (
    'install_dataset',
    'artifact'
);

-- `zones` portion of an `OmicronSledConfig` observed from sled-agent
CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_zone (
    -- where this observation came from
    inv_collection_id UUID NOT NULL,

    -- (foreign key into `inv_omicron_sled_config` table)
    sled_config_id UUID NOT NULL,

    -- unique id for this zone
    id UUID NOT NULL,
    zone_type omicron.public.zone_type NOT NULL,

    -- SocketAddr of the "primary" service for this zone
    -- (what this describes varies by zone type, but all zones have at least one
    -- service in them)
    primary_service_ip INET NOT NULL,
    primary_service_port INT4
        CHECK (primary_service_port BETWEEN 0 AND 65535)
        NOT NULL,

    -- The remaining properties may be NULL for different kinds of zones.  The
    -- specific constraints are not enforced at the database layer, basically
    -- because it's really complicated to do that and it's not obvious that it's
    -- worthwhile.

    -- Some zones have a second service.  Like the primary one, the meaning of
    -- this is zone-type-dependent.
    second_service_ip INET,
    second_service_port INT4
        CHECK (second_service_port IS NULL
        OR second_service_port BETWEEN 0 AND 65535),

    -- Zones may have an associated dataset.  They're currently always on a U.2.
    -- The only thing we need to identify it here is the name of the zpool that
    -- it's on.
    dataset_zpool_name TEXT,

    -- Zones with external IPs have an associated NIC and sockaddr for listening
    -- (first is a foreign key into `inv_omicron_sled_config_zone_nic`)
    nic_id UUID,

    -- Properties for internal DNS servers
    -- address attached to this zone from outside the sled's subnet
    dns_gz_address INET,
    dns_gz_address_index INT8,

    -- Properties for boundary NTP zones
    -- these define upstream servers we need to contact
    ntp_ntp_servers TEXT[],
    ntp_dns_servers INET[],
    ntp_domain TEXT,

    -- Properties specific to Nexus zones
    nexus_external_tls BOOLEAN,
    nexus_external_dns_servers INET ARRAY,

    -- Source NAT configuration (currently used for boundary NTP only)
    snat_ip INET,
    snat_first_port INT4
        CHECK (snat_first_port IS NULL OR snat_first_port BETWEEN 0 AND 65535),
    snat_last_port INT4
        CHECK (snat_last_port IS NULL OR snat_last_port BETWEEN 0 AND 65535),

    -- TODO: This is nullable for backwards compatibility.
    -- Eventually, that nullability should be removed.
    filesystem_pool UUID,

    -- zone image source
    image_source omicron.public.inv_zone_image_source NOT NULL,
    image_artifact_sha256 STRING(64),

    -- Nexus lockstep service port, used only by Nexus zones
    nexus_lockstep_port INT4
        CHECK (nexus_lockstep_port IS NULL OR nexus_lockstep_port BETWEEN 0 AND 65535),

    CONSTRAINT zone_image_source_artifact_hash_present CHECK (
        (image_source = 'artifact'
            AND image_artifact_sha256 IS NOT NULL)
        OR
        (image_source != 'artifact'
            AND image_artifact_sha256 IS NULL)
    ),

    CONSTRAINT nexus_lockstep_port_for_nexus_zones CHECK (
        (zone_type = 'nexus' AND nexus_lockstep_port IS NOT NULL)
        OR
        (zone_type != 'nexus' AND nexus_lockstep_port IS NULL)
    ),

    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);

CREATE INDEX IF NOT EXISTS inv_omicron_sled_config_zone_nic_id
    ON omicron.public.inv_omicron_sled_config_zone (nic_id)
    STORING (
        primary_service_ip,
        second_service_ip,
        snat_ip
    );

CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_zone_nic (
    -- where this observation came from
    inv_collection_id UUID NOT NULL,

    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    ip INET NOT NULL,
    mac INT8 NOT NULL,
    subnet INET NOT NULL,
    vni INT8 NOT NULL,
    is_primary BOOLEAN NOT NULL,
    slot INT2 NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_dataset (
    -- where this observation came from
    inv_collection_id UUID NOT NULL,

    -- foreign key into the `inv_omicron_sled_config` table
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,

    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    -- Only valid if kind = zone
    zone_name TEXT,

    quota INT8,
    reservation INT8,
    compression TEXT NOT NULL,

    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone') OR
      (kind = 'zone' AND zone_name IS NOT NULL)
    ),

    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_sled_config_disk (
    -- where this observation came from
    inv_collection_id UUID NOT NULL,

    -- foreign key into the `inv_omicron_sled_config` table
    sled_config_id UUID NOT NULL,
    id UUID NOT NULL,

    vendor TEXT NOT NULL,
    serial TEXT NOT NULL,
    model TEXT NOT NULL,

    pool_id UUID NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_config_id, id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_clickhouse_keeper_membership (
    inv_collection_id UUID NOT NULL,
    queried_keeper_id INT8 NOT NULL,
    leader_committed_log_index INT8 NOT NULL,
    raft_config INT8[] NOT NULL,

    PRIMARY KEY (inv_collection_id, queried_keeper_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_cockroachdb_status (
    inv_collection_id UUID NOT NULL,
    node_id TEXT NOT NULL,
    ranges_underreplicated INT8,
    liveness_live_nodes INT8,

    PRIMARY KEY (inv_collection_id, node_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_ntp_timesync (
    inv_collection_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    synced BOOL NOT NULL,

    PRIMARY KEY (inv_collection_id, zone_id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_internal_dns (
    inv_collection_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (inv_collection_id, zone_id)
);

/*
 * Various runtime configuration switches for reconfigurator
 *
 * Each configuration option is a single column in a row, and the whole row
 * of configurations is updated atomically. The latest `version` is the active
 * configuration.
 *
 * See https://github.com/oxidecomputer/omicron/issues/8253 for more details.
 */
CREATE TABLE IF NOT EXISTS omicron.public.reconfigurator_config (
    -- Monotonically increasing version for all bp_targets
    version INT8 PRIMARY KEY,

    -- Enable the planner background task
    planner_enabled BOOL NOT NULL DEFAULT FALSE,

    -- The time at which the configuration for a version was set
    time_modified TIMESTAMPTZ NOT NULL,

    -- Whether to add zones while the system has detected a mupdate override.
    add_zones_with_mupdate_override BOOL NOT NULL
);

/*
 * System-level blueprints
 *
 * See RFD 457 and 459 for context.
 *
 * A blueprint describes a potential system configuration. The primary table is
 * the `blueprint` table, which stores only a small amount of metadata about the
 * blueprint. The bulk of the information is stored in the `bp_*` tables below,
 * each of which references back to `blueprint` by ID.
 *
 * `bp_target` describes the "target blueprints" of the system. Insertion must
 * follow a strict set of rules:
 *
 * * The first target blueprint must have version=1, and must have no parent
 *   blueprint.
 * * The Nth target blueprint must have version=N, and its parent blueprint must
 *   be the blueprint that was the target at version=N-1.
 *
 * The result is that the current target blueprint can always be found by
 * looking at the maximally-versioned row in `bp_target`, and there is a linear
 * history from that blueprint all the way back to the version=1 blueprint. We
 * will eventually prune old blueprint targets, so it will not always be
 * possible to view the entire history.
 *
 * `bp_omicron_zone` and `bp_omicron_zone_nic` are nearly identical to their
 * `inv_*` counterparts, and record the `OmicronZoneConfig`s for each sled.
 */

CREATE TYPE IF NOT EXISTS omicron.public.bp_zone_disposition AS ENUM (
    'in_service',
    'expunged'
);

CREATE TYPE IF NOT EXISTS omicron.public.bp_dataset_disposition AS ENUM (
    'in_service',
    'expunged'
);

CREATE TYPE IF NOT EXISTS omicron.public.bp_physical_disk_disposition AS ENUM (
    'in_service',
    'expunged'
);

CREATE TYPE IF NOT EXISTS omicron.public.bp_source AS ENUM (
    'rss',
    'planner',
    'reconfigurator_cli_edit',
    'test'
);

-- list of all blueprints
CREATE TABLE IF NOT EXISTS omicron.public.blueprint (
    id UUID PRIMARY KEY,

    -- This is effectively a foreign key back to this table; however, it is
    -- allowed to be NULL: the initial blueprint has no parent. Additionally,
    -- it may be non-NULL but no longer reference a row in this table: once a
    -- child blueprint has been created from a parent, it's possible for the
    -- parent to be deleted. We do not NULL out this field on such a deletion,
    -- so we can always see that there had been a particular parent even if it's
    -- now gone.
    parent_blueprint_id UUID,

    -- These fields are for debugging only.
    time_created TIMESTAMPTZ NOT NULL,
    creator TEXT NOT NULL,
    comment TEXT NOT NULL,

    -- identifies the latest internal DNS version when blueprint planning began
    internal_dns_version INT8 NOT NULL,
    -- identifies the latest external DNS version when blueprint planning began
    external_dns_version INT8 NOT NULL,
    -- identifies the CockroachDB state fingerprint when blueprint planning began
    cockroachdb_fingerprint TEXT NOT NULL,

    -- CockroachDB settings managed by blueprints.
    --
    -- We use NULL in these columns to reflect that blueprint execution should
    -- not modify the option; we're able to do this because CockroachDB settings
    -- require the value to be the correct type and not NULL. There is no value
    -- that represents "please reset this setting to the default value"; that is
    -- represented by the presence of the default value in that field.
    --
    -- `cluster.preserve_downgrade_option`
    cockroachdb_setting_preserve_downgrade TEXT,

    -- The smallest value of the target_release table's generation field that's
    -- accepted by the blueprint.
    --
    -- For example, let's say that the current target release generation is 5.
    -- Then, when reconfigurator detects a MUPdate:
    --
    -- * the target release is ignored in favor of the install dataset
    -- * this field is set to 6
    --
    -- Once an operator sets a new target release, its generation will be 6 or
    -- higher. Reconfigurator will then know that it is back in charge of
    -- driving the system to the target release.
    --
    -- This is set to 1 by default in application code.
    target_release_minimum_generation INT8 NOT NULL,

    -- The generation of the active group of Nexus instances
    nexus_generation INT8 NOT NULL,

    -- The source of this blueprint
    source omicron.public.bp_source NOT NULL
);

-- table describing both the current and historical target blueprints of the
-- system
CREATE TABLE IF NOT EXISTS omicron.public.bp_target (
    -- Monotonically increasing version for all bp_targets
    version INT8 PRIMARY KEY,

    -- Effectively a foreign key into the `blueprint` table, but may reference a
    -- blueprint that has been deleted (if this target is no longer the current
    -- target: the current target must not be deleted).
    blueprint_id UUID NOT NULL,

    -- Is this blueprint enabled?
    --
    -- Currently, we have no code that acts on this value; however, it exists as
    -- an escape hatch once we have automated blueprint planning and execution.
    -- An operator can set the current blueprint to disabled, which should stop
    -- planning and execution (presumably until a support case can address
    -- whatever issue the update system is causing).
    enabled BOOL NOT NULL,

    -- Timestamp for when this blueprint was made the current target
    time_made_target TIMESTAMPTZ NOT NULL
);

-- metadata associated with a single sled in a blueprint
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_metadata (
    -- foreign key into `blueprint` table
    blueprint_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    sled_state omicron.public.sled_state NOT NULL,
    sled_agent_generation INT8 NOT NULL,
    -- NULL means do not remove any overrides
    remove_mupdate_override UUID,

    -- desired artifact hash for internal disk slots' boot partitions
    -- NULL is translated to
    -- `BlueprintHostPhase2DesiredContents::CurrentContents`
    host_phase_2_desired_slot_a STRING(64),
    host_phase_2_desired_slot_b STRING(64),

    PRIMARY KEY (blueprint_id, sled_id)
);

-- description of omicron physical disks specified in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_physical_disk  (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a blueprint could refer to a sled that no longer exists,
    -- particularly if the blueprint is older than the current target)
    sled_id UUID NOT NULL,

    vendor TEXT NOT NULL,
    serial TEXT NOT NULL,
    model TEXT NOT NULL,

    id UUID NOT NULL,
    pool_id UUID NOT NULL,

    disposition omicron.public.bp_physical_disk_disposition NOT NULL,

     -- Specific properties of the `expunged` disposition
    disposition_expunged_as_of_generation INT,
    disposition_expunged_ready_for_cleanup BOOL NOT NULL,

    PRIMARY KEY (blueprint_id, id),

    CONSTRAINT expunged_disposition_properties CHECK (
      (disposition != 'expunged'
          AND disposition_expunged_as_of_generation IS NULL
          AND NOT disposition_expunged_ready_for_cleanup)
      OR
      (disposition = 'expunged'
          AND disposition_expunged_as_of_generation IS NOT NULL)
    )
);

-- description of an omicron dataset specified in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_dataset (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,

    -- Dataset disposition
    disposition omicron.public.bp_dataset_disposition NOT NULL,

    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    -- Only valid if kind = zone
    zone_name TEXT,

    -- Only valid if kind = crucible
    ip INET,
    port INT4 CHECK (port BETWEEN 0 AND 65535),

    quota INT8,
    reservation INT8,
    compression TEXT NOT NULL,

    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone') OR
      (kind = 'zone' AND zone_name IS NOT NULL)
    ),

    CONSTRAINT ip_and_port_set_for_crucible CHECK (
      (kind != 'crucible') OR
      (kind = 'crucible' AND ip IS NOT NULL and port IS NOT NULL)
    ),

    PRIMARY KEY (blueprint_id, id)
);

CREATE TYPE IF NOT EXISTS omicron.public.bp_zone_image_source AS ENUM (
    'install_dataset',
    'artifact'
);

-- description of omicron zones specified in a blueprint
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a blueprint could refer to a sled that no longer exists,
    -- particularly if the blueprint is older than the current target)
    sled_id UUID NOT NULL,

    -- unique id for this zone
    id UUID NOT NULL,
    zone_type omicron.public.zone_type NOT NULL,

    -- SocketAddr of the "primary" service for this zone
    -- (what this describes varies by zone type, but all zones have at least one
    -- service in them)
    primary_service_ip INET NOT NULL,
    primary_service_port INT4
        CHECK (primary_service_port BETWEEN 0 AND 65535)
        NOT NULL,

    -- The remaining properties may be NULL for different kinds of zones.  The
    -- specific constraints are not enforced at the database layer, basically
    -- because it's really complicated to do that and it's not obvious that it's
    -- worthwhile.

    -- Some zones have a second service.  Like the primary one, the meaning of
    -- this is zone-type-dependent.
    second_service_ip INET,
    second_service_port INT4
        CHECK (second_service_port IS NULL
        OR second_service_port BETWEEN 0 AND 65535),

    -- Zones may have an associated dataset.  They're currently always on a U.2.
    -- The only thing we need to identify it here is the name of the zpool that
    -- it's on.
    dataset_zpool_name TEXT,

    -- Zones with external IPs have an associated NIC and sockaddr for listening
    -- (first is a foreign key into `bp_omicron_zone_nic`)
    bp_nic_id UUID,

    -- Properties for internal DNS servers
    -- address attached to this zone from outside the sled's subnet
    dns_gz_address INET,
    dns_gz_address_index INT8,

    -- Properties for boundary NTP zones
    -- these define upstream servers we need to contact
    ntp_ntp_servers TEXT[],
    ntp_dns_servers INET[],
    ntp_domain TEXT,

    -- Properties specific to Nexus zones
    nexus_external_tls BOOLEAN,
    nexus_external_dns_servers INET ARRAY,

    -- Source NAT configuration (currently used for boundary NTP only)
    snat_ip INET,
    snat_first_port INT4
        CHECK (snat_first_port IS NULL OR snat_first_port BETWEEN 0 AND 65535),
    snat_last_port INT4
        CHECK (snat_last_port IS NULL OR snat_last_port BETWEEN 0 AND 65535),

    -- For some zones, either primary_service_ip or second_service_ip (but not
    -- both!) is an external IP address. For such zones, this is the ID of that
    -- external IP. In general this is a foreign key into
    -- omicron.public.external_ip, though the row many not exist: if this
    -- blueprint is old, it's possible the IP has been deleted, and if this
    -- blueprint has not yet been realized, it's possible the IP hasn't been
    -- created yet.
    external_ip_id UUID,

    filesystem_pool UUID NOT NULL,

    -- Zone disposition
    disposition omicron.public.bp_zone_disposition NOT NULL,

    -- Specific properties of the `expunged` disposition
    disposition_expunged_as_of_generation INT,
    disposition_expunged_ready_for_cleanup BOOL NOT NULL,

    -- Blueprint zone image source
    image_source omicron.public.bp_zone_image_source NOT NULL,
    image_artifact_sha256 STRING(64),

    -- Generation for Nexus zones
    nexus_generation INT8,

    -- Nexus lockstep service port, used only by Nexus zones
    nexus_lockstep_port INT4
        CHECK (nexus_lockstep_port IS NULL OR nexus_lockstep_port BETWEEN 0 AND 65535),

    PRIMARY KEY (blueprint_id, id),

    CONSTRAINT expunged_disposition_properties CHECK (
        (disposition != 'expunged'
            AND disposition_expunged_as_of_generation IS NULL
            AND NOT disposition_expunged_ready_for_cleanup)
        OR
        (disposition = 'expunged'
            AND disposition_expunged_as_of_generation IS NOT NULL)
    ),

    CONSTRAINT zone_image_source_artifact_hash_present CHECK (
        (image_source = 'artifact'
            AND image_artifact_sha256 IS NOT NULL)
        OR
        (image_source != 'artifact'
            AND image_artifact_sha256 IS NULL)
    ),

    CONSTRAINT nexus_generation_for_nexus_zones CHECK (
        (zone_type = 'nexus' AND nexus_generation IS NOT NULL)
        OR
        (zone_type != 'nexus' AND nexus_generation IS NULL)
    ),

    CONSTRAINT nexus_lockstep_port_for_nexus_zones CHECK (
        (zone_type = 'nexus' AND nexus_lockstep_port IS NOT NULL)
        OR
        (zone_type != 'nexus' AND nexus_lockstep_port IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone_nic (
    blueprint_id UUID NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    ip INET NOT NULL,
    mac INT8 NOT NULL,
    subnet INET NOT NULL,
    vni INT8 NOT NULL,
    is_primary BOOLEAN NOT NULL,
    slot INT2 NOT NULL,

    PRIMARY KEY (blueprint_id, id)
);

-- Blueprint information related to clickhouse cluster management
--
-- Rows for this table will only exist for deployments with an existing
-- `ClickhousePolicy` as part of the fleet `Policy`. In the limit, this will be
-- all deployments.
CREATE TABLE IF NOT EXISTS omicron.public.bp_clickhouse_cluster_config (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID PRIMARY KEY,
    -- Generation number to track changes to the cluster state.
    -- Used as optimizitic concurrency control.
    generation INT8 NOT NULL,

    -- Clickhouse server and keeper ids can never be reused. We hand them out
    -- monotonically and keep track of the last one used here.
    max_used_server_id INT8 NOT NULL,
    max_used_keeper_id INT8 NOT NULL,

    -- Each clickhouse cluster has a unique name and secret value. These are set
    -- once and shared among all nodes for the lifetime of the fleet.
    cluster_name TEXT NOT NULL,
    cluster_secret TEXT NOT NULL,

    -- A recording of an inventory value that serves as a marker to inform the
    -- reconfigurator when a collection of a raft configuration is recent.
    highest_seen_keeper_leader_committed_log_index INT8 NOT NULL
);

-- Mapping of an Omicron zone ID to Clickhouse Keeper node ID in a specific
-- blueprint.
--
-- This can logically be considered a subtable of `bp_clickhouse_cluster_config`
CREATE TABLE IF NOT EXISTS omicron.public.bp_clickhouse_keeper_zone_id_to_node_id (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    omicron_zone_id UUID NOT NULL,
    keeper_id INT8 NOT NULL,

    PRIMARY KEY (blueprint_id, omicron_zone_id, keeper_id)
);

-- Mapping of an Omicron zone ID to Clickhouse Server node ID in a specific
-- blueprint.
--
-- This can logically be considered a subtable of `bp_clickhouse_cluster_config`
CREATE TABLE IF NOT EXISTS omicron.public.bp_clickhouse_server_zone_id_to_node_id (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    omicron_zone_id UUID NOT NULL,
    server_id INT8 NOT NULL,

    PRIMARY KEY (blueprint_id, omicron_zone_id, server_id)
);

-- Blueprint information related to which ClickHouse installation
-- oximeter is reading from.
CREATE TABLE IF NOT EXISTS omicron.public.bp_oximeter_read_policy (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID PRIMARY KEY,

    -- Generation number.
    version INT8 NOT NULL,

    -- Which clickhouse installation should oximeter read from.
    oximeter_read_mode omicron.public.oximeter_read_mode NOT NULL
);

-- Blueprint information related to pending RoT bootloader upgrades.
CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_rot_bootloader (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    -- identify of the device to be updated
    -- (foreign key into the `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- location of this device according to MGS
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    -- artifact to be deployed to this device
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    -- RoT bootloader-specific details
    expected_stage0_version STRING NOT NULL,
    expected_stage0_next_version STRING, -- NULL means invalid (no version expected)

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);

-- Blueprint information related to pending SP upgrades.
CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_sp (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    -- identify of the device to be updated
    -- (foreign key into the `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- location of this device according to MGS
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    -- artifact to be deployed to this device
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    -- SP-specific details
    expected_active_version STRING NOT NULL,
    expected_inactive_version STRING, -- NULL means invalid (no version expected)

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);

-- Blueprint information related to pending RoT upgrades.
CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_rot (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    -- identify of the device to be updated
    -- (foreign key into the `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- location of this device according to MGS
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    -- artifact to be deployed to this device
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    -- RoT-specific details
    expected_active_slot omicron.public.hw_rot_slot NOT NULL,
    expected_active_version STRING NOT NULL,
    expected_inactive_version STRING, -- NULL means invalid (no version expected)
    expected_persistent_boot_preference omicron.public.hw_rot_slot NOT NULL,
    expected_pending_persistent_boot_preference omicron.public.hw_rot_slot,
    expected_transient_boot_preference omicron.public.hw_rot_slot,

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);

-- Blueprint information related to pending host OS phase 1 updates.
CREATE TABLE IF NOT EXISTS omicron.public.bp_pending_mgs_update_host_phase_1 (
    -- Foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    -- identify of the device to be updated
    -- (foreign key into the `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- location of this device according to MGS
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,
    -- artifact to be deployed to this device
    artifact_sha256 STRING(64) NOT NULL,
    artifact_version STRING(64) NOT NULL,

    -- host-phase-1-specific details
    expected_active_phase_1_slot omicron.public.hw_m2_slot NOT NULL,
    expected_boot_disk omicron.public.hw_m2_slot NOT NULL,
    expected_active_phase_1_hash STRING(64) NOT NULL,
    expected_active_phase_2_hash STRING(64) NOT NULL,
    expected_inactive_phase_1_hash STRING(64) NOT NULL,
    expected_inactive_phase_2_hash STRING(64) NOT NULL,
    sled_agent_ip INET NOT NULL,
    sled_agent_port INT4 NOT NULL CHECK (sled_agent_port BETWEEN 0 AND 65535),

    PRIMARY KEY(blueprint_id, hw_baseboard_id)
);

-- Mapping of Omicron zone ID to CockroachDB node ID. This isn't directly used
-- by the blueprint tables above, but is used by the more general Reconfigurator
-- system along with them (e.g., to decommission expunged CRDB nodes).
CREATE TABLE IF NOT EXISTS omicron.public.cockroachdb_zone_id_to_node_id (
    omicron_zone_id UUID NOT NULL UNIQUE,
    crdb_node_id TEXT NOT NULL UNIQUE,

    -- We require the pair to be unique, and also require each column to be
    -- unique: there should only be one entry for a given zone ID, one entry for
    -- a given node ID, and we need a unique requirement on the pair (via this
    -- primary key) to support `ON CONFLICT DO NOTHING` idempotent inserts.
    PRIMARY KEY (omicron_zone_id, crdb_node_id)
);

-- Debug logging of blueprint planner reports
--
-- When the blueprint planner inside Nexus runs, it generates a report
-- describing what it did and why. Once the blueprint is generated, this report
-- is only intended for humans for debugging.  Because no shipping software ever
-- never needs to read this and because we want to prioritize ease of evolving
-- these structures, we we punt on a SQL representation entirely. This table
-- stores a JSON blob containing the planning reports for blueprints, but we _do
-- not_ provide any way to parse this data in Nexus or other shipping software.
-- (JSON in the database has all the normal problems of versioning, etc., and we
-- punt on that entirely by saying "do not parse this".) omdb and other dev
-- tooling is free to (attempt to) parse and interpret these JSON blobs.
CREATE TABLE IF NOT EXISTS omicron.public.debug_log_blueprint_planning (
    blueprint_id UUID NOT NULL PRIMARY KEY,
    debug_blob JSONB NOT NULL
);

/*
 * List of debug datasets available for use (e.g., by support bundles).
 *
 * This is a Reconfigurator rendezvous table: it reflects resources that
 * Reconfigurator has ensured exist. It is always possible that a resource
 * chosen from this table could be deleted after it's selected, but any
 * non-deleted row in this table is guaranteed to have been created.
 */
CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_debug_dataset (
    /* ID of the dataset in a blueprint */
    id UUID PRIMARY KEY,

    /* Time this dataset was added to the table */
    time_created TIMESTAMPTZ NOT NULL,

    /*
     * If not NULL, indicates this dataset has been expunged in a blueprint.
     * Multiple Nexus instances operate concurrently, and it's possible any
     * given Nexus is operating on an old blueprint. We need to avoid a Nexus
     * operating on an old blueprint from inserting a dataset that has already
     * been expunged and removed from this table by a later blueprint, so
     * instead of hard deleting, we tombstone rows via this column.
     *
     * Hard deletion of tombstoned datasets will require some care with respect
     * to the problem above. For now we keep tombstoned datasets around forever.
     */
    time_tombstoned TIMESTAMPTZ,

    /* ID of the zpool on which this dataset is placed */
    pool_id UUID NOT NULL,

    /*
     * ID of the target blueprint the Reconfigurator reconciliation RPW was
     * acting on when this row was created.
     *
     * In practice, this will often be the same blueprint ID in which this
     * dataset was added, but it's not guaranteed to be (it could be any
     * descendent blueprint in which this dataset is still in service).
     */
    blueprint_id_when_created UUID NOT NULL,

    /*
     * ID of the target blueprint the Reconfigurator reconciliation RPW was
     * acting on when this row was tombstoned.
     *
     * In practice, this will often be the same blueprint ID in which this
     * dataset was expunged, but it's not guaranteed to be (it could be any
     * descendent blueprint in which this dataset is expunged and not yet
     * pruned).
     */
    blueprint_id_when_tombstoned UUID,

    /*
     * Either both `*_tombstoned` columns should be set (if this row has been
     * tombstoned) or neither should (if it has not).
     */
    CONSTRAINT tombstoned_consistency CHECK (
        (time_tombstoned IS NULL
            AND blueprint_id_when_tombstoned IS NULL)
        OR
        (time_tombstoned IS NOT NULL
            AND blueprint_id_when_tombstoned IS NOT NULL)
    )
);

/* Add an index which lets us find usable debug datasets */
CREATE INDEX IF NOT EXISTS lookup_usable_rendezvous_debug_dataset
    ON omicron.public.rendezvous_debug_dataset (id)
    WHERE time_tombstoned IS NULL;

/*******************************************************************/

/*
 * The `sled_instance` view's definition needs to be modified in a separate
 * transaction from the transaction that created it.
 */

COMMIT;
BEGIN;

-- Describes what happens when
-- (for affinity groups) instance cannot be co-located, or
-- (for anti-affinity groups) instance must be co-located, or
CREATE TYPE IF NOT EXISTS omicron.public.affinity_policy AS ENUM (
    -- If the affinity request cannot be satisfied, fail.
    'fail',

    -- If the affinity request cannot be satisfied, allow it anyway.
    'allow'
);

-- Determines what "co-location" means for instances within an affinity
-- or anti-affinity group.
CREATE TYPE IF NOT EXISTS omicron.public.failure_domain AS ENUM (
    -- Instances are co-located if they are on the same sled.
    'sled'
);

-- Describes a grouping of related instances that should be co-located.
CREATE TABLE IF NOT EXISTS omicron.public.affinity_group (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Affinity groups are contained within projects
    project_id UUID NOT NULL,
    policy omicron.public.affinity_policy NOT NULL,
    failure_domain omicron.public.failure_domain NOT NULL
);

-- Names for affinity groups within a project should be unique
CREATE UNIQUE INDEX IF NOT EXISTS lookup_affinity_group_by_project ON omicron.public.affinity_group (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

-- Describes an instance's membership within an affinity group
CREATE TABLE IF NOT EXISTS omicron.public.affinity_group_instance_membership (
    group_id UUID NOT NULL,
    instance_id UUID NOT NULL,

    PRIMARY KEY (group_id, instance_id)
);

-- We need to look up all memberships of an instance so we can revoke these
-- memberships efficiently when instances are deleted.
CREATE INDEX IF NOT EXISTS lookup_affinity_group_instance_membership_by_instance ON omicron.public.affinity_group_instance_membership (
    instance_id
);

-- Describes a collection of instances that should not be co-located.
CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Anti-Affinity groups are contained within projects
    project_id UUID NOT NULL,
    policy omicron.public.affinity_policy NOT NULL,
    failure_domain omicron.public.failure_domain NOT NULL
);

-- Names for anti-affinity groups within a project should be unique
CREATE UNIQUE INDEX IF NOT EXISTS lookup_anti_affinity_group_by_project ON omicron.public.anti_affinity_group (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

-- Describes an instance's membership within an anti-affinity group
CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group_instance_membership (
    group_id UUID NOT NULL,
    instance_id UUID NOT NULL,

    PRIMARY KEY (group_id, instance_id)
);

-- We need to look up all memberships of an instance so we can revoke these
-- memberships efficiently when instances are deleted.
CREATE INDEX IF NOT EXISTS lookup_anti_affinity_group_instance_membership_by_instance ON omicron.public.anti_affinity_group_instance_membership (
    instance_id
);

CREATE TYPE IF NOT EXISTS omicron.public.vmm_cpu_platform AS ENUM (
  'sled_default',
  'amd_milan',
  'amd_turin'
);

-- Per-VMM state.
CREATE TABLE IF NOT EXISTS omicron.public.vmm (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    instance_id UUID NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,
    sled_id UUID NOT NULL,
    propolis_ip INET NOT NULL,
    propolis_port INT4 NOT NULL CHECK (propolis_port BETWEEN 0 AND 65535) DEFAULT 12400,
    state omicron.public.vmm_state NOT NULL,
    cpu_platform omicron.public.vmm_cpu_platform
);

CREATE INDEX IF NOT EXISTS lookup_vmms_by_sled_id ON omicron.public.vmm (
    sled_id
) WHERE time_deleted IS NULL;

/*
 * A special view of an instance provided to operators for insights into what's
 * running on a sled.
 *
 * This view replaces the placeholder `sled_instance` view defined above. Any
 * columns in the placeholder must appear in the replacement in the same order
 * and with the same types they had in the placeholder.
 */

CREATE OR REPLACE VIEW omicron.public.sled_instance
AS SELECT
   instance.id,
   instance.name,
   silo.name as silo_name,
   project.name as project_name,
   vmm.sled_id as active_sled_id,
   instance.time_created,
   instance.time_modified,
   instance.migration_id,
   instance.ncpus,
   instance.memory,
   vmm.state
FROM
    omicron.public.instance AS instance
    JOIN omicron.public.project AS project ON
            instance.project_id = project.id
    JOIN omicron.public.silo AS silo ON
            project.silo_id = silo.id
    JOIN omicron.public.vmm AS vmm ON
            instance.active_propolis_id = vmm.id
WHERE
    instance.time_deleted IS NULL AND vmm.time_deleted IS NULL;

CREATE SEQUENCE IF NOT EXISTS omicron.public.nat_version START 1 INCREMENT 1;

CREATE TABLE IF NOT EXISTS omicron.public.nat_entry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_address INET NOT NULL,
    first_port INT4 NOT NULL,
    last_port INT4 NOT NULL,
    sled_address INET NOT NULL,
    vni INT4 NOT NULL,
    mac INT8 NOT NULL,
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.nat_version'),
    version_removed INT8,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS nat_version_added ON omicron.public.nat_entry (
    version_added
)
STORING (
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    time_created,
    time_deleted
);

CREATE UNIQUE INDEX IF NOT EXISTS overlapping_nat_entry ON omicron.public.nat_entry (
    external_address,
    first_port,
    last_port
) WHERE time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS nat_lookup ON omicron.public.nat_entry (external_address, first_port, last_port, sled_address, vni, mac);

CREATE UNIQUE INDEX IF NOT EXISTS nat_version_removed ON omicron.public.nat_entry (
    version_removed
)
STORING (
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    time_created,
    time_deleted
);

CREATE INDEX IF NOT EXISTS nat_lookup_by_vni ON omicron.public.nat_entry (
  vni
)
STORING (
  external_address,
  first_port,
  last_port,
  sled_address,
  mac,
  version_added,
  version_removed,
  time_created,
  time_deleted
);

/*
 * A view of the ipv4 nat change history
 * used to summarize changes for external viewing
 */
CREATE VIEW IF NOT EXISTS omicron.public.nat_changes
AS
-- Subquery:
-- We need to be able to order partial changesets. ORDER BY on separate columns
-- will not accomplish this, so we'll do this by interleaving version_added
-- and version_removed (version_removed taking priority if NOT NULL) and then sorting
-- on the appropriate version numbers at call time.
WITH interleaved_versions AS (
  -- fetch all active NAT entries (entries that have not been soft deleted)
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    -- rename version_added to version
    version_added AS version,
    -- create a new virtual column, boolean value representing whether or not
    -- the record has been soft deleted
    (version_removed IS NOT NULL) as deleted
  FROM omicron.public.nat_entry
  WHERE version_removed IS NULL

  -- combine the datasets, unifying the version_added and version_removed
  -- columns to a single `version` column so we can interleave and sort the entries
  UNION

  -- fetch all inactive NAT entries (entries that have been soft deleted)
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    -- rename version_removed to version
    version_removed AS version,
    -- create a new virtual column, boolean value representing whether or not
    -- the record has been soft deleted
    (version_removed IS NOT NULL) as deleted
  FROM omicron.public.nat_entry
  WHERE version_removed IS NOT NULL
)
-- this is our new "table"
-- here we select the columns from the subquery defined above
SELECT
  external_address,
  first_port,
  last_port,
  sled_address,
  vni,
  mac,
  version,
  deleted
FROM interleaved_versions;

CREATE TYPE IF NOT EXISTS omicron.public.bfd_mode AS ENUM (
    'single_hop',
    'multi_hop'
);

CREATE TABLE IF NOT EXISTS omicron.public.bfd_session (
    id UUID PRIMARY KEY,
    local INET,
    remote INET NOT NULL,
    detection_threshold INT8 NOT NULL,
    required_rx INT8 NOT NULL,
    switch TEXT NOT NULL,
    mode  omicron.public.bfd_mode,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_bfd_session ON omicron.public.bfd_session (
    remote,
    switch
) WHERE time_deleted IS NULL;


CREATE TABLE IF NOT EXISTS omicron.public.probe (
    id UUID NOT NULL PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    project_id UUID NOT NULL,
    sled UUID NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_probe_by_name ON omicron.public.probe (
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.upstairs_repair_notification_type AS ENUM (
  'started',
  'succeeded',
  'failed'
);

CREATE TYPE IF NOT EXISTS omicron.public.upstairs_repair_type AS ENUM (
  'live',
  'reconciliation'
);

CREATE TABLE IF NOT EXISTS omicron.public.upstairs_repair_notification (
    time TIMESTAMPTZ NOT NULL,

    repair_id UUID NOT NULL,
    repair_type omicron.public.upstairs_repair_type NOT NULL,

    upstairs_id UUID NOT NULL,
    session_id UUID NOT NULL,

    region_id UUID NOT NULL,
    target_ip INET NOT NULL,
    target_port INT4 CHECK (target_port BETWEEN 0 AND 65535) NOT NULL,

    notification_type omicron.public.upstairs_repair_notification_type NOT NULL,

    /*
     * A repair is uniquely identified by the four UUIDs here, and a
     * notification is uniquely identified by its type.
     */
    PRIMARY KEY (repair_id, upstairs_id, session_id, region_id, notification_type)
);

CREATE TABLE IF NOT EXISTS omicron.public.upstairs_repair_progress (
    repair_id UUID NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    current_item INT8 NOT NULL,
    total_items INT8 NOT NULL,

    PRIMARY KEY (repair_id, time, current_item, total_items)
);

CREATE TYPE IF NOT EXISTS omicron.public.downstairs_client_stop_request_reason_type AS ENUM (
  'replacing',
  'disabled',
  'failed_reconcile',
  'io_error',
  'bad_negotiation_order',
  'incompatible',
  'failed_live_repair',
  'too_many_outstanding_jobs',
  'deactivated'
);

CREATE TABLE IF NOT EXISTS omicron.public.downstairs_client_stop_request_notification (
    time TIMESTAMPTZ NOT NULL,
    upstairs_id UUID NOT NULL,
    downstairs_id UUID NOT NULL,
    reason omicron.public.downstairs_client_stop_request_reason_type NOT NULL,

    PRIMARY KEY (time, upstairs_id, downstairs_id, reason)
);

CREATE TYPE IF NOT EXISTS omicron.public.downstairs_client_stopped_reason_type AS ENUM (
  'connection_timeout',
  'connection_failed',
  'timeout',
  'write_failed',
  'read_failed',
  'requested_stop',
  'finished',
  'queue_closed',
  'receive_task_cancelled'
);

CREATE TABLE IF NOT EXISTS omicron.public.downstairs_client_stopped_notification (
    time TIMESTAMPTZ NOT NULL,
    upstairs_id UUID NOT NULL,
    downstairs_id UUID NOT NULL,
    reason omicron.public.downstairs_client_stopped_reason_type NOT NULL,

    PRIMARY KEY (time, upstairs_id, downstairs_id, reason)
);

CREATE INDEX IF NOT EXISTS rack_initialized ON omicron.public.rack (initialized);

-- table for tracking bootstore configuration changes over time
-- this makes reconciliation easier and also gives us a visible history of changes
CREATE TABLE IF NOT EXISTS omicron.public.bootstore_config (
    key TEXT NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (key, generation),
    data JSONB NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS address_lot_names ON omicron.public.address_lot(name);

CREATE VIEW IF NOT EXISTS omicron.public.bgp_peer_view
AS
SELECT
 sp.switch_location,
 sp.port_name,
 bpc.addr,
 bpc.hold_time,
 bpc.idle_hold_time,
 bpc.delay_open,
 bpc.connect_retry,
 bpc.keepalive,
 bpc.remote_asn,
 bpc.min_ttl,
 bpc.md5_auth_key,
 bpc.multi_exit_discriminator,
 bpc.local_pref,
 bpc.enforce_first_as,
 bpc.vlan_id,
 bc.asn
FROM omicron.public.switch_port sp
JOIN omicron.public.switch_port_settings_bgp_peer_config bpc
ON sp.port_settings_id = bpc.port_settings_id
JOIN omicron.public.bgp_config bc ON bc.id = bpc.bgp_config_id;

CREATE INDEX IF NOT EXISTS switch_port_id_and_name
ON omicron.public.switch_port (port_settings_id, port_name) STORING (switch_location);

CREATE INDEX IF NOT EXISTS switch_port_name ON omicron.public.switch_port (port_name);

CREATE INDEX IF NOT EXISTS network_interface_by_parent
ON omicron.public.network_interface (parent_id)
STORING (name, kind, vpc_id, subnet_id, mac, ip, slot);

CREATE INDEX IF NOT EXISTS sled_by_policy_and_state
ON omicron.public.sled (sled_policy, sled_state, id) STORING (ip);

CREATE INDEX IF NOT EXISTS active_vmm
ON omicron.public.vmm (time_deleted, sled_id, instance_id);

CREATE INDEX IF NOT EXISTS v2p_mapping_details
ON omicron.public.network_interface (
  time_deleted, kind, subnet_id, vpc_id, parent_id
) STORING (mac, ip);

CREATE INDEX IF NOT EXISTS sled_by_policy
ON omicron.public.sled (sled_policy) STORING (ip, sled_state);

CREATE INDEX IF NOT EXISTS vmm_by_instance_id
ON omicron.public.vmm (instance_id) STORING (sled_id);

CREATE TYPE IF NOT EXISTS omicron.public.region_replacement_state AS ENUM (
  'requested',
  'allocating',
  'running',
  'driving',
  'replacement_done',
  'completing',
  'complete'
);

CREATE TABLE IF NOT EXISTS omicron.public.region_replacement (
    /* unique ID for this region replacement */
    id UUID PRIMARY KEY,

    request_time TIMESTAMPTZ NOT NULL,

    old_region_id UUID NOT NULL,

    volume_id UUID NOT NULL,

    old_region_volume_id UUID,

    new_region_id UUID,

    replacement_state omicron.public.region_replacement_state NOT NULL,

    operating_saga_id UUID
);

CREATE INDEX IF NOT EXISTS lookup_region_replacement_by_state on omicron.public.region_replacement (replacement_state);

CREATE TABLE IF NOT EXISTS omicron.public.volume_repair (
    volume_id UUID PRIMARY KEY,
    repair_id UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS lookup_volume_repair_by_repair_id on omicron.public.volume_repair (
    repair_id
);

CREATE TYPE IF NOT EXISTS omicron.public.region_replacement_step_type AS ENUM (
  'propolis',
  'pantry'
);

CREATE TABLE IF NOT EXISTS omicron.public.region_replacement_step (
    replacement_id UUID NOT NULL,

    step_time TIMESTAMPTZ NOT NULL,

    step_type omicron.public.region_replacement_step_type NOT NULL,

    step_associated_instance_id UUID,
    step_associated_vmm_id UUID,

    step_associated_pantry_ip INET,
    step_associated_pantry_port INT4 CHECK (step_associated_pantry_port BETWEEN 0 AND 65535),
    step_associated_pantry_job_id UUID,

    PRIMARY KEY (replacement_id, step_time, step_type)
);

CREATE INDEX IF NOT EXISTS step_time_order on omicron.public.region_replacement_step (step_time);

CREATE INDEX IF NOT EXISTS search_for_repair_notifications ON omicron.public.upstairs_repair_notification (region_id, notification_type);

CREATE INDEX IF NOT EXISTS lookup_any_disk_by_volume_id ON omicron.public.disk (
    volume_id
);

CREATE TYPE IF NOT EXISTS omicron.public.region_snapshot_replacement_state AS ENUM (
  'requested',
  'allocating',
  'replacement_done',
  'deleting_old_volume',
  'running',
  'complete',
  'completing'
);

CREATE TYPE IF NOT EXISTS omicron.public.read_only_target_replacement_type AS ENUM (
  'region_snapshot',
  'read_only_region'
);

CREATE TABLE IF NOT EXISTS omicron.public.region_snapshot_replacement (
    id UUID PRIMARY KEY,

    request_time TIMESTAMPTZ NOT NULL,

    old_dataset_id UUID,
    old_region_id UUID NOT NULL,
    old_snapshot_id UUID,

    old_snapshot_volume_id UUID,

    new_region_id UUID,

    replacement_state omicron.public.region_snapshot_replacement_state NOT NULL,

    operating_saga_id UUID,

    new_region_volume_id UUID,

    replacement_type omicron.public.read_only_target_replacement_type NOT NULL,

    CONSTRAINT proper_replacement_fields CHECK (
      (
       (replacement_type = 'region_snapshot') AND
       ((old_dataset_id IS NOT NULL) AND (old_snapshot_id IS NOT NULL))
      ) OR (
       (replacement_type = 'read_only_region') AND
       ((old_dataset_id IS NULL) AND (old_snapshot_id IS NULL))
      )
    )
);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_replacement_by_state
ON omicron.public.region_snapshot_replacement (replacement_state);

CREATE TYPE IF NOT EXISTS omicron.public.region_snapshot_replacement_step_state AS ENUM (
  'requested',
  'running',
  'complete',
  'volume_deleted'
);

CREATE TABLE IF NOT EXISTS omicron.public.region_snapshot_replacement_step (
    id UUID PRIMARY KEY,

    request_id UUID NOT NULL,

    request_time TIMESTAMPTZ NOT NULL,

    volume_id UUID NOT NULL,

    old_snapshot_volume_id UUID,

    replacement_state omicron.public.region_snapshot_replacement_step_state NOT NULL,

    operating_saga_id UUID
);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_replacement_step_by_state
    on omicron.public.region_snapshot_replacement_step (replacement_state);

CREATE INDEX IF NOT EXISTS lookup_region_snapshot_replacement_step_by_old_volume_id
    on omicron.public.region_snapshot_replacement_step (old_snapshot_volume_id);

-- An allowlist of IP addresses that can make requests to user-facing services.
CREATE TABLE IF NOT EXISTS omicron.public.allow_list (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    -- A nullable list of allowed source IPs.
    --
    -- NULL is used to indicate _any_ source IP is allowed. A _non-empty_ list
    -- represents an explicit allow list of IPs or IP subnets. Note that the
    -- list itself may never be empty.
    allowed_ips INET[] CHECK (array_length(allowed_ips, 1) > 0)
);

-- Insert default allowlist, allowing all traffic.
-- See `schema/crdb/insert-default-allowlist/up.sql` for details.
INSERT INTO omicron.public.allow_list (id, time_created, time_modified, allowed_ips)
VALUES (
    '001de000-a110-4000-8000-000000000000',
    NOW(),
    NOW(),
    NULL
)
ON CONFLICT (id)
DO NOTHING;

CREATE TYPE IF NOT EXISTS omicron.public.migration_state AS ENUM (
  'pending',
  'in_progress',
  'failed',
  'completed'
);

-- A table of the states of current migrations.
CREATE TABLE IF NOT EXISTS omicron.public.migration (
    id UUID PRIMARY KEY,

    /* The ID of the instance that was migrated */
    instance_id UUID NOT NULL,

    /* The time this migration record was created. */
    time_created TIMESTAMPTZ NOT NULL,

    /* The time this migration record was deleted. */
    time_deleted TIMESTAMPTZ,

    /* Note that there's no `time_modified/time_updated` timestamp for migration
     * records. This is because we track updated time separately for the source
     * and target sides of the migration, using separate `time_source_updated`
     * and time_target_updated` columns.
    */

    /* The state of the migration source */
    source_state omicron.public.migration_state NOT NULL,

    /* The ID of the migration source Propolis */
    source_propolis_id UUID NOT NULL,

    /* Generation number owned and incremented by the source sled-agent */
    source_gen INT8 NOT NULL DEFAULT 1,

    /* Timestamp of when the source field was last updated.
     *
     * This is provided by the sled-agent when publishing a migration state
     * update.
     */
    time_source_updated TIMESTAMPTZ,

    /* The state of the migration target */
    target_state omicron.public.migration_state NOT NULL,

    /* The ID of the migration target Propolis */
    target_propolis_id UUID NOT NULL,

    /* Generation number owned and incremented by the target sled-agent */
    target_gen INT8 NOT NULL DEFAULT 1,

    /* Timestamp of when the source field was last updated.
     *
     * This is provided by the sled-agent when publishing a migration state
     * update.
     */
    time_target_updated TIMESTAMPTZ
);

/* Lookup migrations by instance ID */
CREATE INDEX IF NOT EXISTS lookup_migrations_by_instance_id ON omicron.public.migration (
    instance_id
);

/* Migrations by time created.
 *
 * Currently, this is only used by OMDB for ordering the `omdb migration list`
 * output, but it may be used by other UIs in the future...
*/
CREATE INDEX IF NOT EXISTS migrations_by_time_created ON omicron.public.migration (
    time_created
);

/* Lookup region snapshot by snapshot id */
CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_snapshot_id on omicron.public.region_snapshot (
    snapshot_id
);

CREATE INDEX IF NOT EXISTS lookup_bgp_config_by_bgp_announce_set_id ON omicron.public.bgp_config (
    bgp_announce_set_id
) WHERE
    time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.volume_resource_usage_type AS ENUM (
  'read_only_region',
  'region_snapshot'
);

/*
 * This table records when a Volume makes use of a read-only resource. When
 * there are no more entries for a particular read-only resource, then that
 * resource can be garbage collected.
 */
CREATE TABLE IF NOT EXISTS omicron.public.volume_resource_usage (
    usage_id UUID NOT NULL,

    volume_id UUID NOT NULL,

    usage_type omicron.public.volume_resource_usage_type NOT NULL,

    /*
     * This column contains a non-NULL value when the usage type is read_only
     * region
     */
    region_id UUID,

    /*
     * These columns contain non-NULL values when the usage type is region
     * snapshot
     */
    region_snapshot_dataset_id UUID,
    region_snapshot_region_id UUID,
    region_snapshot_snapshot_id UUID,

    PRIMARY KEY (usage_id),

    CONSTRAINT exactly_one_usage_source CHECK (
     (
      (usage_type = 'read_only_region') AND
      (region_id IS NOT NULL) AND
      (
       region_snapshot_dataset_id IS NULL AND
       region_snapshot_region_id IS NULL AND
       region_snapshot_snapshot_id IS NULL
      )
     )
    OR
     (
      (usage_type = 'region_snapshot') AND
      (region_id IS NULL) AND
      (
       region_snapshot_dataset_id IS NOT NULL AND
       region_snapshot_region_id IS NOT NULL AND
       region_snapshot_snapshot_id IS NOT NULL
      )
     )
    )
);

CREATE INDEX IF NOT EXISTS lookup_volume_resource_usage_by_region on omicron.public.volume_resource_usage (
    region_id
);

CREATE INDEX IF NOT EXISTS lookup_volume_resource_usage_by_snapshot on omicron.public.volume_resource_usage (
    region_snapshot_dataset_id, region_snapshot_region_id, region_snapshot_snapshot_id
);

CREATE UNIQUE INDEX IF NOT EXISTS one_record_per_volume_resource_usage on omicron.public.volume_resource_usage (
    volume_id,
    usage_type,
    region_id,
    region_snapshot_dataset_id,
    region_snapshot_region_id,
    region_snapshot_snapshot_id
);

CREATE TYPE IF NOT EXISTS omicron.public.audit_log_actor_kind AS ENUM (
    'user_builtin',
    'silo_user',
    'unauthenticated'
);

CREATE TYPE IF NOT EXISTS omicron.public.audit_log_result_kind AS ENUM (
    'success',
    'error',
    -- represents the case where we had to clean up a row and artificially
    -- complete it in order to get it into the log (because entries don't show
    -- up in the log until they're completed)
    'timeout'
);

CREATE TABLE IF NOT EXISTS omicron.public.audit_log (
    id UUID PRIMARY KEY,
    time_started TIMESTAMPTZ NOT NULL,
    -- request IDs are UUIDs but let's give them a little extra space
    -- https://github.com/oxidecomputer/dropshot/blob/83f78e7/dropshot/src/server.rs#L743
    request_id STRING(63) NOT NULL,
    request_uri STRING(512) NOT NULL,
    operation_id STRING(512) NOT NULL,
    source_ip INET NOT NULL,
    -- Pulled from request header if present and truncated
    user_agent STRING(256),

    -- these are all null if the request is unauthenticated. actor_id can
    -- be present while silo ID is null if the user is built in (non-silo).
    actor_id UUID,
    actor_silo_id UUID,
    -- actor kind indicating builtin user, silo user, or unauthenticated
    actor_kind omicron.public.audit_log_actor_kind NOT NULL,
    -- The name of the authn scheme used
    auth_method STRING(63),

    -- below are fields we can only fill in after the operation

    time_completed TIMESTAMPTZ,
    http_status_code INT4,

    -- only present on errors
    error_code STRING,
    error_message STRING,

    -- result kind indicating success, error, or timeout
    result_kind omicron.public.audit_log_result_kind,

    -- make sure time_completed and result_kind are either both null or both not
    CONSTRAINT time_completed_and_result_kind CHECK (
        (time_completed IS NULL AND result_kind IS NULL)
        OR (time_completed IS NOT NULL AND result_kind IS NOT NULL)
    ),

    -- Enforce consistency between result_kind and related fields:
    -- 'timeout': no HTTP status or error details
    -- 'success': requires HTTP status, no error details
    -- 'error': requires HTTP status and error message
    -- other/NULL: no HTTP status or error details
    CONSTRAINT result_kind_state_consistency CHECK (
        CASE result_kind
            WHEN 'timeout' THEN http_status_code IS NULL AND error_code IS NULL
                AND error_message IS NULL
            WHEN 'success' THEN error_code IS NULL AND error_message IS NULL AND
                http_status_code IS NOT NULL
            WHEN 'error' THEN http_status_code IS NOT NULL AND error_message IS
                NOT NULL
            ELSE http_status_code IS NULL AND error_code IS NULL AND error_message
                IS NULL
        END
    ),

    -- Ensure valid actor ID combinations
    -- Constraint: actor_kind and actor_id must be consistent
    CONSTRAINT actor_kind_and_id_consistent CHECK (
        -- For user_builtin: must have actor_id, must not have actor_silo_id
        (actor_kind = 'user_builtin' AND actor_id IS NOT NULL AND actor_silo_id IS NULL)
        OR
        -- For silo_user: must have both actor_id and actor_silo_id
        (actor_kind = 'silo_user' AND actor_id IS NOT NULL AND actor_silo_id IS NOT NULL)
        OR
        -- For unauthenticated: must not have actor_id or actor_silo_id
        (actor_kind = 'unauthenticated' AND actor_id IS NULL AND actor_silo_id IS NULL)
    )
);

-- When we query the audit log, we filter by time_completed and order by
-- (time_completed, id). CRDB docs talk about hash-sharded indexes for
-- sequential keys, but the PK on this table is the ID alone.
CREATE UNIQUE INDEX IF NOT EXISTS audit_log_by_time_completed
    ON omicron.public.audit_log (time_completed, id)
    WHERE time_completed IS NOT NULL;

-- View of audit log entries that have been "completed". This lets us treat that
-- subset of rows as its own table in the data model code. Completing an entry
-- means updating the entry after an operation is complete with the result of
-- the operation. Because we do not intend to fail or roll back the operation
-- if the completion write fails (while we do abort if the audit log entry
-- initialization call fails), it is always possible (though rare) that there
-- will be some incomplete entries remaining for operations that have in fact
-- completed. We intend to complete those periodically with some kind of job
-- and most likely mark them with a special third status that is neither success
-- nor failure.
CREATE VIEW IF NOT EXISTS omicron.public.audit_log_complete AS
SELECT
    id,
    time_started,
    request_id,
    request_uri,
    operation_id,
    source_ip,
    user_agent,
    actor_id,
    actor_silo_id,
    actor_kind,
    auth_method,
    time_completed,
    http_status_code,
    error_code,
    error_message,
    result_kind
FROM omicron.public.audit_log
WHERE
    time_completed IS NOT NULL
    AND result_kind IS NOT NULL;

/*
 * Alerts
 */


/*
 * Alert webhook receivers, receiver secrets, and receiver subscriptions.
 */

CREATE TABLE IF NOT EXISTS omicron.public.alert_receiver (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Child resource generation for secrets.
    secret_gen INT NOT NULL,

    -- Child resource generation for subscriptions. This is separate from
    -- `secret_gen`, as updating secrets and updating subscriptions are separate
    -- operations which don't conflict with each other.
    subscription_gen INT NOT NULL,
    -- URL of the endpoint webhooks are delivered to.
    endpoint STRING(512) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_alert_rx_by_id
ON omicron.public.alert_receiver (id)
WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lookup_alert_rx_by_name
ON omicron.public.alert_receiver (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE IF NOT EXISTS omicron.public.webhook_secret (
    -- ID of this secret.
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    -- N.B. that this will always be equal to `time_created` for secrets, as
    -- they are never modified once created.
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- Secret value.
    secret STRING(512) NOT NULL
);

CREATE INDEX IF NOT EXISTS lookup_webhook_secrets_by_rx
ON omicron.public.webhook_secret (
    rx_id
) WHERE
    time_deleted IS NULL;

-- Alert classes.
--
-- When creating new alert classes, be sure to add them here!
CREATE TYPE IF NOT EXISTS omicron.public.alert_class AS ENUM (
    -- Liveness probes, which are technically not real alerts, but, you know...
    'probe',
    -- Test classes used to test globbing.
    --
    -- These are not publicly exposed.
    'test.foo',
    'test.foo.bar',
    'test.foo.baz',
    'test.quux.bar',
    'test.quux.bar.baz'
    -- Add new alert classes here!
);

-- The set of alert class filters (either alert class names or alert class glob
-- patterns) associated with a alert receiver.
--
-- This is used when creating entries in the alert_subscription table to
-- indicate that a alert receiver is interested in a given event class.
CREATE TABLE IF NOT EXISTS omicron.public.alert_glob (
    -- UUID of the alert receiver (foreign key into
    -- `omicron.public.alert_receiver`)
    rx_id UUID NOT NULL,
    -- An event class glob to which this receiver is subscribed.
    glob STRING(512) NOT NULL,
    -- Regex used when evaluating this filter against concrete alert classes.
    regex STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    -- The database schema version at which this glob was last expanded.
    --
    -- This is used to detect when a glob must be re-processed to generate exact
    -- subscriptions on schema changes.
    --
    -- If this is NULL, no exact subscriptions have been generated for this glob
    -- yet (i.e. it was just created)
    schema_version STRING(64),

    PRIMARY KEY (rx_id, glob)
);

-- Look up all event class globs for an alert receiver.
CREATE INDEX IF NOT EXISTS lookup_alert_globs_for_rx
ON omicron.public.alert_glob (
    rx_id
);

CREATE INDEX IF NOT EXISTS lookup_alert_globs_by_schema_version
ON omicron.public.alert_glob (schema_version);

CREATE TABLE IF NOT EXISTS omicron.public.alert_subscription (
    -- UUID of the alert receiver (foreign key into
    -- `omicron.public.alert_receiver`)
    rx_id UUID NOT NULL,
    -- An alert class to which the receiver is subscribed.
    alert_class omicron.public.alert_class NOT NULL,
    -- If this subscription is a concrete instantiation of a glob pattern, the
    -- value of the glob that created it (and, a foreign key into
    -- `webhook_rx_event_glob`). If the receiver is subscribed to this exact
    -- event class, then this is NULL.
    --
    -- This is used when deleting a glob subscription, as it is necessary to
    -- delete any concrete subscriptions to individual event classes matching
    -- that glob.
    glob STRING(512),

    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (rx_id, alert_class)
);

-- Look up all receivers subscribed to an alert class. This is used by
-- the dispatcher to determine who is interested in a particular event.
CREATE INDEX IF NOT EXISTS lookup_alert_rxs_for_class
ON omicron.public.alert_subscription (
    alert_class
);

-- Look up all exact event class subscriptions for a receiver.
--
-- This is used when generating a view of all user-provided original
-- subscriptions provided for a receiver. That list is generated by looking up
-- all exact event class subscriptions for the receiver ID in this table,
-- combined with the list of all globs in the `alert_glob` table.
CREATE INDEX IF NOT EXISTS lookup_exact_subscriptions_for_alert_rx
on omicron.public.alert_subscription (
    rx_id
) WHERE glob IS NULL;

/*
 * Alert message queue.
 */

CREATE TABLE IF NOT EXISTS omicron.public.alert (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    -- The class of alert that this is.
    alert_class omicron.public.alert_class NOT NULL,
    -- Actual alert data. The structure of this depends on the alert class.
    payload JSONB NOT NULL,

    -- Set when dispatch entries have been created for this alert.
    time_dispatched TIMESTAMPTZ,
    -- The number of receivers that this alart was dispatched to.
    num_dispatched INT8 NOT NULL,

    CONSTRAINT time_dispatched_set_if_dispatched CHECK (
        (num_dispatched = 0) OR (time_dispatched IS NOT NULL)
    ),

    CONSTRAINT num_dispatched_is_positive CHECK (
        (num_dispatched >= 0)
    )
);

-- Singleton probe alert
INSERT INTO omicron.public.alert (
    id,
    time_created,
    time_modified,
    alert_class,
    payload,
    time_dispatched,
    num_dispatched
) VALUES (
    -- NOTE: this UUID is duplicated in nexus_db_model::alert.
    '001de000-7768-4000-8000-000000000001',
    NOW(),
    NOW(),
    'probe',
    '{}',
    -- Pretend to be dispatched so we won't show up in "list alerts needing
    -- dispatch" queries
    NOW(),
    0
) ON CONFLICT DO NOTHING;

-- Look up webhook events in need of dispatching.
--
-- This is used by the message dispatcher when looking for events to dispatch.
CREATE INDEX IF NOT EXISTS lookup_undispatched_alerts
ON omicron.public.alert (
    id, time_created
) WHERE time_dispatched IS NULL;


/*
 * Alert message dispatching and delivery attempts.
 */

-- Describes why an alert delivery was triggered
CREATE TYPE IF NOT EXISTS omicron.public.alert_delivery_trigger AS ENUM (
    --  This delivery was triggered by the alert being dispatched.
    'alert',
    -- This delivery was triggered by an explicit call to the alert resend API.
    'resend',
    --- This delivery is a liveness probe.
    'probe'
);

-- Describes the state of an alert delivery
CREATE TYPE IF NOT EXISTS omicron.public.alert_delivery_state AS ENUM (
    --  This delivery has not yet completed.
    'pending',
    -- This delivery has failed.
    'failed',
    --- This delivery has completed successfully.
    'delivered'
);

-- Delivery dispatch table for webhook receivers.
CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery (
    -- UUID of this delivery.
    id UUID PRIMARY KEY,
    --- UUID of the alert (foreign key into `omicron.public.alert`).
    alert_id UUID NOT NULL,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.alert_receiver`)
    rx_id UUID NOT NULL,

    triggered_by omicron.public.alert_delivery_trigger NOT NULL,

    --- Delivery attempt count. Starts at 0.
    attempts INT2 NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,
    -- If this is set, then this webhook message has either been delivered
    -- successfully, or is considered permanently failed.
    time_completed TIMESTAMPTZ,

    state omicron.public.alert_delivery_state NOT NULL,

    -- Deliverator coordination bits
    deliverator_id UUID,
    time_leased TIMESTAMPTZ,

    CONSTRAINT attempts_is_non_negative CHECK (attempts >= 0),
    CONSTRAINT active_deliveries_have_started_timestamps CHECK (
        (deliverator_id IS NULL) OR (
            deliverator_id IS NOT NULL AND time_leased IS NOT NULL
        )
    ),
    CONSTRAINT time_completed_iff_not_pending CHECK (
        (state = 'pending' AND time_completed IS NULL) OR
            (state != 'pending' AND time_completed IS NOT NULL)
    )
);

-- Ensure that initial delivery attempts (nexus-dispatched) are unique to avoid
-- duplicate work when an alert is dispatched. For deliveries created by calls
-- to the webhook event resend API, we don't enforce this constraint, to allow
-- re-delivery to be triggered multiple times.
CREATE UNIQUE INDEX IF NOT EXISTS one_webhook_event_dispatch_per_rx
ON omicron.public.webhook_delivery (
    alert_id, rx_id
)
WHERE
    triggered_by = 'alert';

-- Index for looking up all webhook messages dispatched to a receiver ID
CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_dispatched_to_rx
ON omicron.public.webhook_delivery (
    rx_id, alert_id
);

-- Index for looking up all delivery attempts for an alert
CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_for_alert
ON omicron.public.webhook_delivery (
    alert_id
);

-- Index for looking up all currently in-flight webhook messages, and ordering
-- them by their creation times.
CREATE INDEX IF NOT EXISTS webhook_deliveries_in_flight
ON omicron.public.webhook_delivery (
    time_created, id
) WHERE
    time_completed IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.webhook_delivery_attempt_result as ENUM (
    -- The delivery attempt failed with an HTTP error.
    'failed_http_error',
    -- The delivery attempt failed because the receiver endpoint was
    -- unreachable.
    'failed_unreachable',
    --- The delivery attempt connected successfully but no response was received
    --  within the timeout.
    'failed_timeout',
    -- The delivery attempt succeeded.
    'succeeded'
);

CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery_attempt (
    -- Primary key
    id UUID PRIMARY KEY,
    -- Foreign key into `omicron.public.webhook_delivery`.
    delivery_id UUID NOT NULL,
    -- attempt number.
    attempt INT2 NOT NULL,

    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,

    result omicron.public.webhook_delivery_attempt_result NOT NULL,

    -- This is an INT4 to ensure we can store any unsigned 16-bit number,
    -- although status code > 599 would be Very Surprising...
    response_status INT4,
    response_duration INTERVAL,
    time_created TIMESTAMPTZ NOT NULL,
    -- UUID of the Nexus who did this delivery attempt.
    deliverator_id UUID NOT NULL,

    -- Attempt numbers start at 1
    CONSTRAINT attempts_start_at_1 CHECK (attempt >= 1),

    -- Ensure response status codes are not negative.
    -- We could be more prescriptive here, and also check that they're >= 100
    -- and <= 599, but some servers may return weird stuff, and we'd like to be
    -- able to record that they did that.
    CONSTRAINT response_status_is_unsigned CHECK (
        (response_status IS NOT NULL AND response_status >= 0) OR
            (response_status IS NULL)
    ),

    CONSTRAINT response_iff_not_unreachable CHECK (
        (
            -- If the result is 'succeedeed' or 'failed_http_error', response
            -- data must be present.
            (result = 'succeeded' OR result = 'failed_http_error') AND (
                response_status IS NOT NULL AND
                response_duration IS NOT NULL
            )
        ) OR (
            -- If the result is 'failed_unreachable' or 'failed_timeout', no
            -- response data is present.
            (result = 'failed_unreachable' OR result = 'failed_timeout') AND (
                response_status IS NULL AND
                response_duration IS NULL
            )
        )
    )
);

CREATE INDEX IF NOT EXISTS lookup_attempts_for_webhook_delivery
ON omicron.public.webhook_delivery_attempt (
    delivery_id
);

CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_attempts_to_rx
ON omicron.public.webhook_delivery_attempt (
    rx_id
);

CREATE TYPE IF NOT EXISTS omicron.public.user_data_export_resource_type AS ENUM (
  'snapshot',
  'image'
);

CREATE TYPE IF NOT EXISTS omicron.public.user_data_export_state AS ENUM (
  'requested',
  'assigning',
  'live',
  'deleting',
  'deleted'
);

/*
 * This table contains a record when a snapshot is being exported.
 */
CREATE TABLE IF NOT EXISTS omicron.public.user_data_export (
    id UUID PRIMARY KEY,

    state omicron.public.user_data_export_state NOT NULL,
    operating_saga_id UUID,
    generation INT8 NOT NULL,

    resource_id UUID NOT NULL,
    resource_type omicron.public.user_data_export_resource_type NOT NULL,
    resource_deleted BOOL NOT NULL,

    pantry_ip INET,
    pantry_port INT4 CHECK (pantry_port BETWEEN 0 AND 65535),
    volume_id UUID
);

CREATE INDEX IF NOT EXISTS lookup_export_by_resource_type
ON omicron.public.user_data_export (resource_type);

CREATE UNIQUE INDEX IF NOT EXISTS one_export_record_per_resource
ON omicron.public.user_data_export (resource_id)
WHERE state != 'deleted';

CREATE INDEX IF NOT EXISTS lookup_export_by_volume
ON omicron.public.user_data_export (volume_id);

CREATE INDEX IF NOT EXISTS lookup_export_by_state
ON omicron.public.user_data_export (state);

/*
 * Ereports
 *
 * See RFD 520 for details:
 * https://rfd.shared.oxide.computer/rfd/520
 */

/* Ereports from service processors */
CREATE TABLE IF NOT EXISTS omicron.public.sp_ereport (
    /*
     * the primary key for an ereport is formed from the tuple of the
     * reporter's restart ID (a randomly generated UUID) and the ereport's ENA
     * (a 64-bit integer that uniquely identifies the ereport within that
     * restart of the reporter).
     *
     * see: https://rfd.shared.oxide.computer/rfd/520#ereport-metadata
     */
    restart_id UUID NOT NULL,
    ena INT8 NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* time at which the ereport was collected */
    time_collected TIMESTAMPTZ NOT NULL,
    /* UUID of the Nexus instance that collected the ereport */
    collector_id UUID NOT NULL,

    /*
     * physical location of the reporting SP
     *
     * these fields are always present, as they are how requests to collect
     * ereports are indexed by MGS.
     */
    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,

    /*
     * VPD identity of the reporting SP.
     *
     * unlike the physical location, these fields are nullable, as an ereport
     * may be generated in a state where the SP doesn't know who or what it is.
     * consider that "i don't know my own identity" is a reasonable condition
     * to want to generate an ereport about!
     */
    serial_number STRING,
    part_number STRING,

    /*
     * The ereport class, which indicates the category of event reported.
     *
     * This is nullable, as it is extracted from the report JSON, and reports
     * missing class information must still be ingested.
     */
    class STRING,

    /*
     * JSON representation of the ereport as received from the SP.
     *
     * the raw JSON representation of the ereport is always stored, alongside
     * any more structured data that we extract from it, as extracting data
     * from the received ereport requires additional knowledge of the ereport
     * formats generated by the SP and its various tasks. as these may change,
     * and new ereports may be added which Nexus may not yet be aware of,
     * we always store the raw JSON representation of the ereport. as Nexus
     * becomes aware of new ereport schemas, it can go back and extract
     * structured data from previously collected ereports with those schemas,
     * but this is only possible if the JSON blob is persisted.
     *
     * see also: https://rfd.shared.oxide.computer/rfd/520#data-model
     */
    report JSONB NOT NULL,

    PRIMARY KEY (restart_id, ena)
);

CREATE INDEX IF NOT EXISTS lookup_sp_ereports_by_slot
ON omicron.public.sp_ereport (
    sp_type,
    sp_slot,
    time_collected
)
where
    time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS order_sp_ereports_by_timestamp
ON omicron.public.sp_ereport(
    time_collected
)
WHERE
    time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS lookup_sp_ereports_by_serial
ON omicron.public.sp_ereport (
    serial_number
) WHERE
    time_deleted IS NULL;

/* Ereports from the host operating system */
CREATE TABLE IF NOT EXISTS omicron.public.host_ereport (
    /*
    * the primary key for an ereport is formed from the tuple of the
    * reporter's restart ID (a randomly generated UUID) and the ereport's ENA
    * (a 64-bit integer that uniquely identifies the ereport within that
    * restart of the reporter).
    *
    * see: https://rfd.shared.oxide.computer/rfd/520#ereport-metadata
    */
    restart_id UUID NOT NULL,
    ena INT8 NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* time at which the ereport was collected */
    time_collected TIMESTAMPTZ NOT NULL,
    /* UUID of the Nexus instance that collected the ereport */
    collector_id UUID NOT NULL,

    /* identity of the reporting sled */
    sled_id UUID NOT NULL,
    sled_serial TEXT NOT NULL,

    /*
     * The ereport class, which indicates the category of event reported.
     *
     * This is nullable, as it is extracted from the report JSON, and reports
     * missing class information must still be ingested.
     */
    class STRING,

    /*
     * JSON representation of the ereport as received from the sled-agent.
     *
     * the raw JSON representation of the ereport is always stored, alongside
     * any more structured data that we extract from it, as extracting data
     * from the received ereport requires additional knowledge of the ereport
     * formats generated by the host OS' fault management system. as these may
     * change, and new ereports may be added which Nexus may not yet be aware
     * of, we always store the raw JSON representation of the ereport. as Nexus
     * becomes aware of new ereport schemas, it can go back and extract
     * structured data from previously collected ereports with those schemas,
     * but this is only possible if the JSON blob is persisted.
     *
     * see also: https://rfd.shared.oxide.computer/rfd/520#data-model
     */
    report JSONB NOT NULL,

    part_number STRING(63),

    PRIMARY KEY (restart_id, ena)
);

CREATE INDEX IF NOT EXISTS lookup_host_ereports_by_sled
ON omicron.public.host_ereport (
    sled_id,
    time_collected
)
WHERE
    time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS order_host_ereports_by_timestamp
ON omicron.public.host_ereport (
    time_collected
)
WHERE
    time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS lookup_host_ereports_by_serial
ON omicron.public.host_ereport (
    sled_serial
) WHERE
    time_deleted IS NULL;

-- Metadata for the schema itself.
--
-- This table may be read by Nexuses with different notions of "what the schema should be".
-- Unlike other tables in the database, caution should be taken when upgrading this schema.
CREATE TABLE IF NOT EXISTS omicron.public.db_metadata (
    -- There should only be one row of this table for the whole DB.
    -- It's a little goofy, but filter on "singleton = true" before querying
    -- or applying updates, and you'll access the singleton row.
    --
    -- We also add a constraint on this table to ensure it's not possible to
    -- access the version of this table with "singleton = false".
    singleton BOOL NOT NULL PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    -- Semver representation of the DB version
    version STRING(64) NOT NULL,

    -- (Optional) Semver representation of the DB version to which we're upgrading
    target_version STRING(64),

    CHECK (singleton = true)
);

CREATE TYPE IF NOT EXISTS omicron.public.db_metadata_nexus_state AS ENUM (
    -- This Nexus is allowed to access this database
    'active',

    -- This Nexus is not yet allowed to access the database
    'not_yet',

    -- This Nexus has committed to no longer accessing this database
    'quiesced'
);

-- Nexuses which may be attempting to access the database, and a state
-- which identifies if they should be allowed to do so.
--
-- This table is used during upgrade implement handoff between old and new
-- Nexus zones. It is read by all Nexuses during initialization to identify
-- if they should have access to the database.
CREATE TABLE IF NOT EXISTS omicron.public.db_metadata_nexus (
    nexus_id UUID NOT NULL PRIMARY KEY,
    last_drained_blueprint_id UUID,
    state omicron.public.db_metadata_nexus_state NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_db_metadata_nexus_by_state on omicron.public.db_metadata_nexus (
    state,
    nexus_id
);

-- Keep this at the end of file so that the database does not contain a version
-- until it is fully populated.
INSERT INTO omicron.public.db_metadata (
    singleton,
    time_created,
    time_modified,
    version,
    target_version
) VALUES
    (TRUE, NOW(), NOW(), '199.0.0', NULL)
ON CONFLICT DO NOTHING;

COMMIT;
