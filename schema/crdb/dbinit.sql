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
    sled_agent_gen INT8 NOT NULL DEFAULT 1
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

CREATE TYPE IF NOT EXISTS omicron.public.sled_resource_kind AS ENUM (
    -- omicron.public.instance
    'instance'
    -- We expect to other resource kinds here in the future; e.g., to track
    -- resources used by control plane services. For now, we only track
    -- instances.
);

-- Accounting for programs using resources on a sled
CREATE TABLE IF NOT EXISTS omicron.public.sled_resource (
    -- Should match the UUID of the corresponding resource
    id UUID PRIMARY KEY,

    -- The sled where resources are being consumed
    sled_id UUID NOT NULL,

    -- The maximum number of hardware threads usable by this resource
    hardware_threads INT8 NOT NULL,

    -- The maximum amount of RSS RAM provisioned to this resource
    rss_ram INT8 NOT NULL,

    -- The maximum amount of Reservoir RAM provisioned to this resource
    reservoir_ram INT8 NOT NULL,

    -- Identifies the type of the resource
    kind omicron.public.sled_resource_kind NOT NULL
);

-- Allow looking up all resources which reside on a sled
CREATE UNIQUE INDEX IF NOT EXISTS lookup_resource_by_sled ON omicron.public.sled_resource (
    sled_id,
    id
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
    disk_state omicron.public.physical_disk_state NOT NULL
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
    physical_disk_id UUID NOT NULL
);

/* Create an index on the physical disk id */
CREATE INDEX IF NOT EXISTS lookup_zpool_by_disk on omicron.public.zpool (
    physical_disk_id,
    id
) WHERE physical_disk_id IS NOT NULL AND time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.dataset_kind AS ENUM (
  'crucible',
  'cockroach',
  'clickhouse',
  'clickhouse_keeper',
  'external_dns',
  'internal_dns'
);

/*
 * A dataset of allocated space within a zpool.
 */
CREATE TABLE IF NOT EXISTS omicron.public.dataset (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Pool table */
    pool_id UUID NOT NULL,

    /* Contact information for the dataset */
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    kind omicron.public.dataset_kind NOT NULL,

    /* An upper bound on the amount of space that might be in-use */
    size_used INT,

    /* Crucible must make use of 'size_used'; other datasets manage their own storage */
    CONSTRAINT size_used_column_set_for_crucible CHECK (
      (kind != 'crucible') OR
      (kind = 'crucible' AND size_used IS NOT NULL)
    )
);

/* Create an index on the size usage for Crucible's allocation */
CREATE INDEX IF NOT EXISTS lookup_dataset_by_size_used_crucible on omicron.public.dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL AND kind = 'crucible';

/* Create an index on the size usage for any dataset */
CREATE INDEX IF NOT EXISTS lookup_dataset_by_size_used on omicron.public.dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL;

/* Create an index on the zpool id */
CREATE INDEX IF NOT EXISTS lookup_dataset_by_zpool on omicron.public.dataset (
    pool_id,
    id
) WHERE pool_id IS NOT NULL AND time_deleted IS NULL;

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
    extent_count INT NOT NULL
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

/* Index for use during join with region table */
CREATE INDEX IF NOT EXISTS lookup_region_by_dataset on omicron.public.region_snapshot (
    dataset_id, region_id
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
  'jit'
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
    rcgen INT NOT NULL
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
    external_id TEXT NOT NULL
);

/* This index lets us quickly find users for a given silo. */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_silo_user_by_silo ON omicron.public.silo_user (
    silo_id,
    external_id
) WHERE
    time_deleted IS NULL;

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
    external_id TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_silo_group_by_silo ON omicron.public.silo_group (
    silo_id,
    external_id
) WHERE
    time_deleted IS NULL;

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
    storage_bytes INT8 NOT NULL
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

CREATE TYPE IF NOT EXISTS omicron.public.instance_state AS ENUM (
    'creating',
    'starting',
    'running',
    'stopping',
    'stopped',
    'rebooting',
    'migrating',
    'repairing',
    'failed',
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

    /* The state of the instance when it has no active VMM. */
    state omicron.public.instance_state NOT NULL,
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
    boot_on_fault BOOL NOT NULL DEFAULT false,

    /* ID of the instance update saga that has locked this instance for
     * updating, if one exists. */
    updater_id UUID,

    /* Generation of the instance updater lock */
    updater_gen INT NOT NULL DEFAULT 0

);

-- Names for instances within a project should be unique
CREATE UNIQUE INDEX IF NOT EXISTS lookup_instance_by_project ON omicron.public.instance (
    project_id,
    name
) WHERE
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

CREATE UNIQUE INDEX IF NOT EXISTS lookup_snapshot_by_project ON omicron.public.snapshot (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Oximeter collector servers.
 */
CREATE TABLE IF NOT EXISTS omicron.public.oximeter (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL
);

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
    'instance'
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
    ipv6_block INET NOT NULL
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
    'service'
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
    is_primary BOOL NOT NULL
);

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
    is_primary
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

CREATE TYPE IF NOT EXISTS omicron.public.vpc_firewall_rule_protocol AS ENUM (
    'TCP',
    'UDP',
    'ICMP'
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
    filter_protocols omicron.public.vpc_firewall_rule_protocol[],
    action omicron.public.vpc_firewall_rule_action NOT NULL,
    priority INT4 CHECK (priority BETWEEN 0 AND 65535) NOT NULL
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
    rcgen INT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_router_by_vpc ON omicron.public.vpc_router (
    vpc_id,
    name
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

    vpc_router_id UUID NOT NULL,
    kind omicron.public.router_route_kind NOT NULL,
    target STRING(128) NOT NULL,
    destination STRING(128) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_route_by_router ON omicron.public.router_route (
    vpc_router_id,
    name
) WHERE
    time_deleted IS NULL;

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
    rcgen INT8 NOT NULL
);

/*
 * Index ensuring uniqueness of IP Pool names, globally.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_by_name ON omicron.public.ip_pool (
    name
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
    ip_pool_id UUID NOT NULL,
    /* Tracks child resources, IP addresses allocated out of this range. */
    rcgen INT8 NOT NULL
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
    'done'
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
    token STRING(40) PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);

-- to be used for cleaning up old tokens
-- It's okay that this index is non-unique because we don't need to page through
-- this list.  We'll just grab the next N, delete them, then repeat.
CREATE INDEX IF NOT EXISTS lookup_console_by_creation ON omicron.public.console_session (
    time_created
);

-- This index is used to remove sessions for a user that's being deleted.
CREATE INDEX IF NOT EXISTS lookup_console_by_silo_user ON omicron.public.console_session (
    silo_user_id
);

/*******************************************************************/

-- Describes a single uploaded TUF repo.
--
-- Identified by both a random uuid and its SHA256 hash. The hash could be the
-- primary key, but it seems unnecessarily large and unwieldy.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,

    sha256 STRING(64) NOT NULL,

    -- The version of the targets.json role that was used to generate the repo.
    targets_role_version INT NOT NULL,

    -- The valid_until time for the repo.
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

    CONSTRAINT unique_checksum UNIQUE (sha256),
    CONSTRAINT unique_system_version UNIQUE (system_version)
);

-- Describes an individual artifact from an uploaded TUF repo.
--
-- In the future, this may also be used to describe artifacts that are fetched
-- from a remote TUF repo, but that requires some additional design work.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_artifact (
    name STRING(63) NOT NULL,
    version STRING(63) NOT NULL,
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

    PRIMARY KEY (name, version, kind)
);

-- Reflects that a particular artifact was provided by a particular TUF repo.
-- This is a many-many mapping.
CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_artifact (
    tuf_repo_id UUID NOT NULL,
    tuf_artifact_name STRING(63) NOT NULL,
    tuf_artifact_version STRING(63) NOT NULL,
    tuf_artifact_kind STRING(63) NOT NULL,

    /*
    For the primary key, this definition uses the natural key rather than a
    smaller surrogate key (UUID). That's because with CockroachDB the most
    important factor in selecting a primary key is the ability to distribute
    well. In this case, the first element of the primary key is the tuf_repo_id,
    which is a random UUID.

    For more, see https://www.cockroachlabs.com/blog/how-to-choose-a-primary-key/.
    */
    PRIMARY KEY (
        tuf_repo_id, tuf_artifact_name, tuf_artifact_version, tuf_artifact_kind
    )
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
    time_expires TIMESTAMPTZ NOT NULL
);

-- Access tokens granted in response to successful device authorization flows.
CREATE TABLE IF NOT EXISTS omicron.public.device_access_token (
    token STRING(40) PRIMARY KEY,
    client_id UUID NOT NULL,
    device_code STRING(40) NOT NULL,
    silo_user_id UUID NOT NULL,
    time_requested TIMESTAMPTZ NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ
);

-- This UNIQUE constraint is critical for ensuring that at most
-- one token is ever created for a given device authorization flow.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_device_access_token_by_client ON omicron.public.device_access_token (
    client_id, device_code
);

-- This index is used to remove tokens for a user that's being deleted.
CREATE INDEX IF NOT EXISTS lookup_device_access_token_by_silo_user ON omicron.public.device_access_token (
    silo_user_id
);

/*
 * Roles built into the system
 *
 * You can think of a built-in role as an opaque token to which we assign a
 * hardcoded set of permissions.  The role that we call "project.viewer"
 * corresponds to the "viewer" role on the "project" resource.  A user that has
 * this role on a particular Project is granted various read-only permissions on
 * that Project.  The specific permissions associated with the role are defined
 * in Omicron's Polar (Oso) policy file.
 *
 * A built-in role like "project.viewer" has four parts:
 *
 * * resource type: "project"
 * * role name: "viewer"
 * * full name: "project.viewer"
 * * description: "Project Viewer"
 *
 * Internally, we can treat the tuple (resource type, role name) as a composite
 * primary key.  Externally, we expose this as the full name.  This is
 * consistent with RFD 43 and other IAM systems.
 *
 * These fields look awfully close to the identity metadata that we use for most
 * other tables.  But they're just different enough that we can't use most of
 * the same abstractions:
 *
 * * "id": We have no need for a uuid because the (resource_type, role_name) is
 *   already unique and immutable.
 * * "name": What we call "full name" above could instead be called "name",
 *   which would be consistent with other identity metadata.  But it's not a
 *   legal "name" because of the period, and it would be confusing to have
 *   "resource type", "role name", and "name".
 * * "time_created": not that useful because it's whenever the system was
 *   initialized, and we have plenty of other timestamps for that
 * * "time_modified": does not apply because the role cannot be changed
 * * "time_deleted" does not apply because the role cannot be deleted
 *
 * If the set of roles and their permissions are fixed, why store them in the
 * database at all?  Because what's dynamic is the assignment of roles to users.
 * We have a separate table that says "user U has role ROLE on resource
 * RESOURCE".  How do we represent the ROLE part of this association?  We use a
 * foreign key into this "role_builtin" table.
 */
CREATE TABLE IF NOT EXISTS omicron.public.role_builtin (
    resource_type STRING(63),
    role_name STRING(63),
    description STRING(512),

    PRIMARY KEY(resource_type, role_name)
);

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
    /* Composite foreign key into "role_builtin" table */
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

CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_link_config (
    port_settings_id UUID,
    lldp_service_config_id UUID NOT NULL,
    link_name TEXT,
    mtu INT4,

    PRIMARY KEY (port_settings_id, link_name)
);

CREATE TABLE IF NOT EXISTS omicron.public.lldp_service_config (
    id UUID PRIMARY KEY,
    lldp_config_id UUID,
    enabled BOOL NOT NULL
);

CREATE TABLE IF NOT EXISTS omicron.public.lldp_config (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    chassis_id TEXT,
    system_name TEXT,
    system_description TEXT,
    management_ip TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS lldp_config_by_name ON omicron.public.lldp_config (
    name
) WHERE
    time_deleted IS NULL;

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
    version TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS caboose_properties
    on omicron.public.sw_caboose (board, git_commit, name, version);

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

    PRIMARY KEY (inv_collection_id, sled_id)
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

    -- FK consisting of:
    -- - Which collection this was
    -- - The sled reporting the disk
    -- - The slot in which this disk was found
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

CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_omicron_zones (
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

    -- OmicronZonesConfig generation reporting these zones
    generation INT8 NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id)
);

CREATE TYPE IF NOT EXISTS omicron.public.zone_type AS ENUM (
  'boundary_ntp',
  'clickhouse',
  'clickhouse_keeper',
  'cockroach_db',
  'crucible',
  'crucible_pantry',
  'external_dns',
  'internal_dns',
  'internal_ntp',
  'nexus',
  'oximeter'
);

-- observations from sled agents about Omicron-managed zones
CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_zone (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- unique id for this zone
    id UUID NOT NULL,
    underlay_address INET NOT NULL,
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
    -- (first is a foreign key into `inv_omicron_zone_nic`)
    nic_id UUID,

    -- Properties for internal DNS servers
    -- address attached to this zone from outside the sled's subnet
    dns_gz_address INET,
    dns_gz_address_index INT8,

    -- Properties common to both kinds of NTP zones
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

    PRIMARY KEY (inv_collection_id, id)
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_omicron_zone_nic (
    inv_collection_id UUID NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    ip INET NOT NULL,
    mac INT8 NOT NULL,
    subnet INET NOT NULL,
    vni INT8 NOT NULL,
    is_primary BOOLEAN NOT NULL,
    slot INT2 NOT NULL,

    PRIMARY KEY (inv_collection_id, id)
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
 * `bp_sled_omicron_zones`, `bp_omicron_zone`, and `bp_omicron_zone_nic` are
 * nearly identical to their `inv_*` counterparts, and record the
 * `OmicronZonesConfig` for each sled.
 */

CREATE TYPE IF NOT EXISTS omicron.public.bp_zone_disposition AS ENUM (
    'in_service',
    'quiesced',
    'expunged'
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
    cockroachdb_setting_preserve_downgrade TEXT
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

-- state of a sled in a blueprint
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_state (
    -- foreign key into `blueprint` table
    blueprint_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    sled_state omicron.public.sled_state NOT NULL,
    PRIMARY KEY (blueprint_id, sled_id)
);

-- description of a collection of omicron physical disks stored in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_omicron_physical_disks (
    -- foreign key into `blueprint` table
    blueprint_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    generation INT8 NOT NULL,
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

    PRIMARY KEY (blueprint_id, id)
);

-- see inv_sled_omicron_zones, which is identical except it references a
-- collection whereas this table references a blueprint
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_omicron_zones (
    -- foreign key into `blueprint` table
    blueprint_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (blueprint_id, sled_id)
);

-- description of omicron zones specified in a blueprint
--
-- This is currently identical to `inv_omicron_zone`, except that the foreign
-- keys reference other blueprint tables intead of inventory tables. We expect
-- their sameness to diverge over time as either inventory or blueprints (or
-- both) grow context-specific properties.
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a blueprint could refer to a sled that no longer exists,
    -- particularly if the blueprint is older than the current target)
    sled_id UUID NOT NULL,

    -- unique id for this zone
    id UUID NOT NULL,
    underlay_address INET NOT NULL,
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

    -- Properties common to both kinds of NTP zones
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

    -- Zone disposition
    disposition omicron.public.bp_zone_disposition NOT NULL,

    -- For some zones, either primary_service_ip or second_service_ip (but not
    -- both!) is an external IP address. For such zones, this is the ID of that
    -- external IP. In general this is a foreign key into
    -- omicron.public.external_ip, though the row many not exist: if this
    -- blueprint is old, it's possible the IP has been deleted, and if this
    -- blueprint has not yet been realized, it's possible the IP hasn't been
    -- created yet.
    external_ip_id UUID,

    PRIMARY KEY (blueprint_id, id)
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

/*******************************************************************/

/*
 * The `sled_instance` view's definition needs to be modified in a separate
 * transaction from the transaction that created it.
 */

COMMIT;
BEGIN;

-- Per-VMM state.
CREATE TABLE IF NOT EXISTS omicron.public.vmm (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    instance_id UUID NOT NULL,
    state omicron.public.instance_state NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,
    sled_id UUID NOT NULL,
    propolis_ip INET NOT NULL,
    propolis_port INT4 NOT NULL CHECK (propolis_port BETWEEN 0 AND 65535) DEFAULT 12400
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

ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS fec omicron.public.switch_link_fec;
ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS speed omicron.public.switch_link_speed;

CREATE SEQUENCE IF NOT EXISTS omicron.public.ipv4_nat_version START 1 INCREMENT 1;

CREATE TABLE IF NOT EXISTS omicron.public.ipv4_nat_entry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_address INET NOT NULL,
    first_port INT4 NOT NULL,
    last_port INT4 NOT NULL,
    sled_address INET NOT NULL,
    vni INT4 NOT NULL,
    mac INT8 NOT NULL,
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.ipv4_nat_version'),
    version_removed INT8,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ipv4_nat_version_added ON omicron.public.ipv4_nat_entry (
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

CREATE UNIQUE INDEX IF NOT EXISTS overlapping_ipv4_nat_entry ON omicron.public.ipv4_nat_entry (
    external_address,
    first_port,
    last_port
) WHERE time_deleted IS NULL;

CREATE INDEX IF NOT EXISTS ipv4_nat_lookup ON omicron.public.ipv4_nat_entry (external_address, first_port, last_port, sled_address, vni, mac);

CREATE UNIQUE INDEX IF NOT EXISTS ipv4_nat_version_removed ON omicron.public.ipv4_nat_entry (
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

ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS autoneg BOOL NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS ipv4_nat_lookup_by_vni ON omicron.public.ipv4_nat_entry (
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
CREATE VIEW IF NOT EXISTS omicron.public.ipv4_nat_changes
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
  FROM omicron.public.ipv4_nat_entry
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
  FROM omicron.public.ipv4_nat_entry
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

ALTER TABLE omicron.public.external_ip ADD COLUMN IF NOT EXISTS is_probe BOOL NOT NULL DEFAULT false;

ALTER TYPE omicron.public.network_interface_kind ADD VALUE IF NOT EXISTS 'probe';

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

COMMIT;
BEGIN;

-- view for v2p mapping rpw
CREATE VIEW IF NOT EXISTS omicron.public.v2p_mapping_view
AS
WITH VmV2pMappings AS (
  SELECT
    n.id as nic_id,
    s.id as sled_id,
    s.ip as sled_ip,
    v.vni,
    n.mac,
    n.ip
  FROM omicron.public.network_interface n
  JOIN omicron.public.vpc_subnet vs ON vs.id = n.subnet_id
  JOIN omicron.public.vpc v ON v.id = n.vpc_id
  JOIN omicron.public.vmm vmm ON n.parent_id = vmm.instance_id
  JOIN omicron.public.sled s ON vmm.sled_id = s.id
  WHERE n.time_deleted IS NULL
  AND n.kind = 'instance'
  AND (vmm.state = 'running' OR vmm.state = 'starting')
  AND s.sled_policy = 'in_service'
  AND s.sled_state = 'active'
),
ProbeV2pMapping AS (
  SELECT
    n.id as nic_id,
    s.id as sled_id,
    s.ip as sled_ip,
    v.vni,
    n.mac,
    n.ip
  FROM omicron.public.network_interface n
  JOIN omicron.public.vpc_subnet vs ON vs.id = n.subnet_id
  JOIN omicron.public.vpc v ON v.id = n.vpc_id
  JOIN omicron.public.probe p ON n.parent_id = p.id
  JOIN omicron.public.sled s ON p.sled = s.id
  WHERE n.time_deleted IS NULL
  AND n.kind = 'probe'
  AND s.sled_policy = 'in_service'
  AND s.sled_state = 'active'
)
SELECT nic_id, sled_id, sled_ip, vni, mac, ip FROM VmV2pMappings
UNION
SELECT nic_id, sled_id, sled_ip, vni, mac, ip FROM ProbeV2pMapping;

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

CREATE INDEX IF NOT EXISTS lookup_snapshot_by_destination_volume_id ON omicron.public.snapshot ( destination_volume_id );

/*
 * Metadata for the schema itself. This version number isn't great, as there's
 * nothing to ensure it gets bumped when it should be, but it's a start.
 */
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


/*
 * Keep this at the end of file so that the database does not contain a version
 * until it is fully populated.
 */
INSERT INTO omicron.public.db_metadata (
    singleton,
    time_created,
    time_modified,
    version,
    target_version
) VALUES
    (TRUE, NOW(), NOW(), '69.0.0', NULL)
ON CONFLICT DO NOTHING;

COMMIT;
