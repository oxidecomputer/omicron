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

CREATE TYPE IF NOT EXISTS omicron.public.sled_provision_state AS ENUM (
    -- New resources can be provisioned onto the sled
    'provisionable',
    -- New resources must not be provisioned onto the sled
    'non_provisionable'
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

    /* The last address allocated to an Oxide service on this sled. */
    last_used_address INET NOT NULL,

    /* The state of whether resources should be provisioned onto the sled */
    provision_state omicron.public.sled_provision_state NOT NULL,

    -- This constraint should be upheld, even for deleted disks
    -- in the fleet.
    CONSTRAINT serial_part_revision_unique UNIQUE (
      serial_number, part_number, revision
    )
);

/* Add an index which lets us look up sleds on a rack */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_sled_by_rack ON omicron.public.sled (
    rack_id,
    id
) WHERE time_deleted IS NULL;

CREATE TYPE IF NOT EXISTS omicron.public.sled_resource_kind AS ENUM (
    -- omicron.public.dataset
    'dataset',
    -- omicron.public.service
    'service',
    -- omicron.public.instance
    'instance',
    -- omicron.public.sled
    --
    -- reserved as an approximation of sled internal usage, such as "by the OS
    -- and all unaccounted services".
    'reserved'
);

-- Accounting for programs using resources on a sled
CREATE TABLE IF NOT EXISTS omicron.public.sled_resource (
    -- Should match the UUID of the corresponding service
    id UUID PRIMARY KEY,

    -- The sled where resources are being consumed
    sled_id UUID NOT NULL,

    -- Identifies the type of the resource
    kind omicron.public.sled_resource_kind NOT NULL,

    -- The maximum number of hardware threads usable by this resource
    hardware_threads INT8 NOT NULL,

    -- The maximum amount of RSS RAM provisioned to this resource
    rss_ram INT8 NOT NULL,

    -- The maximum amount of Reservoir RAM provisioned to this resource
    reservoir_ram INT8 NOT NULL
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

CREATE TABLE IF NOT EXISTS omicron.public.service (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /* FK into the Sled table */
    sled_id UUID NOT NULL,
    /* For services in illumos zones, the zone's unique id (for debugging) */
    zone_id UUID,
    /* The IP address of the service. */
    ip INET NOT NULL,
    /* The UDP or TCP port on which the service listens. */
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,
    /* Indicates the type of service. */
    kind omicron.public.service_kind NOT NULL
);

/* Add an index which lets us look up the services on a sled */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_service_by_sled ON omicron.public.service (
    sled_id,
    id
);

/* Look up (and paginate) services of a given kind. */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_service_by_kind ON omicron.public.service (
    kind,
    id
);

CREATE TYPE IF NOT EXISTS omicron.public.physical_disk_kind AS ENUM (
  'm2',
  'u2'
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

    -- This constraint should be upheld, even for deleted disks
    -- in the fleet.
    CONSTRAINT vendor_serial_model_unique UNIQUE (
      vendor, serial, model
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_physical_disk_by_variant ON omicron.public.physical_disk (
    variant,
    id
) WHERE time_deleted IS NULL;

-- Make it efficient to look up physical disks by Sled.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_physical_disk_by_sled ON omicron.public.physical_disk (
    sled_id,
    id
) WHERE time_deleted IS NULL;

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

/*
 * ZPools of Storage, attached to Sleds.
 * These are backed by a single physical disk.
 */
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

    total_size INT NOT NULL
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
    q.storage_bytes AS storage_allocated
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
    boot_on_fault BOOL NOT NULL DEFAULT false
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
    -- A service in the omicron.public.service table
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
    /* TODO: Is this length appropriate? */
    base_route STRING(512) NOT NULL,
    /* Oximeter collector instance to which this metric producer is assigned. */
    oximeter_id UUID NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_producer_by_oximeter ON omicron.public.metric_producer (
    oximeter_id,
    id
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
    rcgen INT8 NOT NULL,

    /*
     * Association with a silo. silo_id is also used to mark an IP pool as
     * "internal" by associating it with the oxide-internal silo. Null silo_id
     * means the pool is can be used fleet-wide.
     */
    silo_id UUID,

    /* Is this the default pool for its scope (fleet or silo) */
    is_default BOOLEAN NOT NULL DEFAULT FALSE
);

/*
 * Ensure there can only be one default pool for the fleet or a given silo.
 * Coalesce is needed because otherwise different nulls are considered to be
 * distinct from each other.
 */
CREATE UNIQUE INDEX IF NOT EXISTS one_default_pool_per_scope ON omicron.public.ip_pool (
    COALESCE(silo_id, '00000000-0000-0000-0000-000000000000'::uuid)
) WHERE
    is_default = true AND time_deleted IS NULL;

/*
 * Index ensuring uniqueness of IP Pool names, globally.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_pool_by_name ON omicron.public.ip_pool (
    name
) WHERE
    time_deleted IS NULL;

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
     * Only nullable if this is a floating IP, which may exist not
     * attached to any instance or service yet.
     */
    CONSTRAINT null_non_fip_parent_id CHECK (
        (kind != 'floating' AND parent_id is NOT NULL) OR (kind = 'floating')
    ),

    /* Ephemeral IPs are not supported for services. */
    CONSTRAINT ephemeral_kind_service CHECK (
        (kind = 'ephemeral' AND is_service = FALSE) OR (kind != 'ephemeral')
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

CREATE TYPE IF NOT EXISTS omicron.public.update_artifact_kind AS ENUM (
    -- Sled artifacts
    'gimlet_sp',
    'gimlet_rot',
    'host',
    'trampoline',
    'control_plane',

    -- PSC artifacts
    'psc_sp',
    'psc_rot',

    -- Switch artifacts
    'switch_sp',
    'switch_rot'
);

CREATE TABLE IF NOT EXISTS omicron.public.update_artifact (
    name STRING(63) NOT NULL,
    version STRING(63) NOT NULL,
    kind omicron.public.update_artifact_kind NOT NULL,

    /* the version of the targets.json role this came from */
    targets_role_version INT NOT NULL,

    /* when the metadata this artifact was cached from expires */
    valid_until TIMESTAMPTZ NOT NULL,

    /* data about the target from the targets.json role */
    target_name STRING(512) NOT NULL,
    target_sha256 STRING(64) NOT NULL,
    target_length INT NOT NULL,

    PRIMARY KEY (name, version, kind)
);

/* This index is used to quickly find outdated artifacts. */
CREATE INDEX IF NOT EXISTS lookup_artifact_by_targets_role_version ON omicron.public.update_artifact (
    targets_role_version
);

/*
 * System updates
 */
CREATE TABLE IF NOT EXISTS omicron.public.system_update (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- Because the version is unique, it could be the PK, but that would make
    -- this resource different from every other resource for little benefit.

    -- Unique semver version
    version STRING(64) NOT NULL -- TODO: length
);

CREATE UNIQUE INDEX IF NOT EXISTS lookup_update_by_version ON omicron.public.system_update (
    version
);

 
CREATE TYPE IF NOT EXISTS omicron.public.updateable_component_type AS ENUM (
    'bootloader_for_rot',
    'bootloader_for_sp',
    'bootloader_for_host_proc',
    'hubris_for_psc_rot',
    'hubris_for_psc_sp',
    'hubris_for_sidecar_rot',
    'hubris_for_sidecar_sp',
    'hubris_for_gimlet_rot',
    'hubris_for_gimlet_sp',
    'helios_host_phase_1',
    'helios_host_phase_2',
    'host_omicron'
);

/*
 * Component updates. Associated with at least one system_update through
 * system_update_component_update.
 */
CREATE TABLE IF NOT EXISTS omicron.public.component_update (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- On component updates there's no device ID because the update can apply to
    -- multiple instances of a given device kind

    -- The *system* update version associated with this version (this is confusing, will rename)
    version STRING(64) NOT NULL, -- TODO: length
    -- TODO: add component update version to component_update

    component_type omicron.public.updateable_component_type NOT NULL
);

-- version is unique per component type
CREATE UNIQUE INDEX IF NOT EXISTS lookup_component_by_type_and_version ON omicron.public.component_update (
    component_type, version
);

/*
 * Associate system updates with component updates. Not done with a
 * system_update_id field on component_update because the same component update
 * may be part of more than one system update.
 */
CREATE TABLE IF NOT EXISTS omicron.public.system_update_component_update (
    system_update_id UUID NOT NULL,
    component_update_id UUID NOT NULL,

    PRIMARY KEY (system_update_id, component_update_id)
);

-- For now, the plan is to treat stopped, failed, completed as sub-cases of
-- "steady" described by a "reason". But reason is not implemented yet.
-- Obviously this could be a boolean, but boolean status fields never stay
-- boolean for long.
CREATE TYPE IF NOT EXISTS omicron.public.update_status AS ENUM (
    'updating',
    'steady'
);

/*
 * Updateable components and their update status
 */
CREATE TABLE IF NOT EXISTS omicron.public.updateable_component (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- Free-form string that comes from the device
    device_id STRING(40) NOT NULL,

    component_type omicron.public.updateable_component_type NOT NULL,

    -- The semver version of this component's own software
    version STRING(64) NOT NULL, -- TODO: length

    -- The version of the system update this component's software came from.
    -- This may need to be nullable if we are registering components before we
    -- know about system versions at all
    system_version STRING(64) NOT NULL, -- TODO: length

    status omicron.public.update_status NOT NULL
    -- TODO: status reason for updateable_component
);

-- can't have two components of the same type with the same device ID
CREATE UNIQUE INDEX IF NOT EXISTS lookup_component_by_type_and_device ON omicron.public.updateable_component (
    component_type, device_id
);

CREATE INDEX IF NOT EXISTS lookup_component_by_system_version ON omicron.public.updateable_component (
    system_version
);

/*
 * System updates
 */
CREATE TABLE IF NOT EXISTS omicron.public.update_deployment (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- semver version of corresponding system update
    -- TODO: this makes sense while version is the PK of system_update, but
    -- if/when I change that back to ID, this needs to be the ID too
    version STRING(64) NOT NULL,

    status omicron.public.update_status NOT NULL
    -- TODO: status reason for update_deployment
);

CREATE INDEX IF NOT EXISTS lookup_deployment_by_creation on omicron.public.update_deployment (
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

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, interface_name, addr)
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
    bgp_announce_set_id UUID NOT NULL
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

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);

CREATE TYPE IF NOT EXISTS omicron.public.caboose_which AS ENUM (
    'sp_slot_0',
    'sp_slot_1',
    'rot_slot_A',
    'rot_slot_B'
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

/*******************************************************************/

/*
 * The `sled_instance` view's definition needs to be modified in a separate
 * transaction from the transaction that created it.
 */

COMMIT;
BEGIN;

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
    propolis_ip INET NOT NULL
);

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

ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS autoneg BOOL NOT NULL DEFAULT false;

INSERT INTO omicron.public.db_metadata (
    singleton,
    time_created,
    time_modified,
    version,
    target_version
) VALUES
    ( TRUE, NOW(), NOW(), '22.0.0', NULL)
ON CONFLICT DO NOTHING;

COMMIT;
