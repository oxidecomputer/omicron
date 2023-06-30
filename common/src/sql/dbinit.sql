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

/*
 * We assume the database and user do not already exist so that we don't
 * inadvertently clobber what's there.  If they might exist, the user has to
 * clear this first.
 *
 * NOTE: the database and user names MUST be kept in sync with the
 * initialization code and dbwipe.sql.
 */
CREATE DATABASE omicron;
CREATE USER omicron;
ALTER DEFAULT PRIVILEGES GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES to omicron;

/*
 * Racks
 */
CREATE TABLE omicron.public.rack (
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
    tuf_base_url STRING(512)
);

/*
 * Sleds
 */

CREATE TABLE omicron.public.sled (
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

    -- This constraint should be upheld, even for deleted disks
    -- in the fleet.
    CONSTRAINT serial_part_revision_unique UNIQUE (
      serial_number, part_number, revision
    )
);

/* Add an index which lets us look up sleds on a rack */
CREATE INDEX ON omicron.public.sled (
    rack_id
) WHERE time_deleted IS NULL;

CREATE INDEX ON omicron.public.sled (
    id
) WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.sled_resource_kind AS ENUM (
    -- omicron.public.Dataset
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
CREATE TABLE omicron.public.sled_resource (
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
CREATE INDEX ON omicron.public.sled_resource (
    sled_id
);

/*
 * Switches
 */

CREATE TABLE omicron.public.switch (
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
CREATE INDEX ON omicron.public.switch (
    rack_id
) WHERE time_deleted IS NULL;

CREATE INDEX ON omicron.public.switch (
    id
) WHERE
    time_deleted IS NULL;

/*
 * Services
 */

CREATE TYPE omicron.public.service_kind AS ENUM (
  'clickhouse',
  'cockroach',
  'crucible',
  'crucible_pantry',
  'dendrite',
  'external_dns',
  'internal_dns',
  'nexus',
  'ntp',
  'oximeter',
  'tfport'
);

CREATE TABLE omicron.public.service (
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
CREATE INDEX ON omicron.public.service (
    sled_id,
    id
);

CREATE INDEX ON omicron.public.service (
    kind,
    id
);

CREATE TYPE omicron.public.physical_disk_kind AS ENUM (
  'm2',
  'u2'
);

-- A physical disk which exists inside the rack.
CREATE TABLE omicron.public.physical_disk (
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

CREATE INDEX ON omicron.public.physical_disk (
    variant,
    id
) WHERE time_deleted IS NULL;

-- Make it efficient to look up physical disks by Sled.
CREATE INDEX ON omicron.public.physical_disk (
    sled_id,
    id
) WHERE time_deleted IS NULL;

-- x509 certificates which may be used by services
CREATE TABLE omicron.public.certificate (
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
CREATE INDEX ON omicron.public.certificate (
    service,
    id
) WHERE
    time_deleted IS NULL;

-- Add an index which enforces that certificates have unique names, and which
-- allows pagination-by-name.
CREATE UNIQUE INDEX ON omicron.public.certificate (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

-- A table describing virtual resource provisioning which may be associated
-- with a collection of objects, including:
-- - Projects
-- - Silos
-- - Fleet
CREATE TABLE omicron.public.virtual_provisioning_collection (
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
CREATE TABLE omicron.public.virtual_provisioning_resource (
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
CREATE TABLE omicron.public.Zpool (
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

CREATE TYPE omicron.public.dataset_kind AS ENUM (
  'crucible',
  'cockroach',
  'clickhouse',
  'external_dns',
  'internal_dns'
);

/*
 * A dataset of allocated space within a zpool.
 */
CREATE TABLE omicron.public.Dataset (
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
CREATE INDEX on omicron.public.Dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL AND kind = 'crucible';

/* Create an index on the size usage for any dataset */
CREATE INDEX on omicron.public.Dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL;

/*
 * A region of space allocated to Crucible Downstairs, within a dataset.
 */
CREATE TABLE omicron.public.Region (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /* FK into the Dataset table */
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
CREATE INDEX on omicron.public.Region (
    volume_id
);

/*
 * Allow all regions belonging to a dataset to be accessed quickly.
 */
CREATE INDEX on omicron.public.Region (
    dataset_id
);

/*
 * A snapshot of a region, within a dataset.
 */
CREATE TABLE omicron.public.region_snapshot (
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

    PRIMARY KEY (dataset_id, region_id, snapshot_id)
);

/* Index for use during join with region table */
CREATE INDEX on omicron.public.region_snapshot (
    dataset_id, region_id
);

/*
 * Index on volume_references and snapshot_addr for crucible
 * resource accounting lookup
 */
CREATE INDEX on omicron.public.region_snapshot (
    volume_references
);

CREATE INDEX on omicron.public.region_snapshot (
    snapshot_addr
);

/*
 * A volume within Crucible
 */
CREATE TABLE omicron.public.volume (
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
CREATE INDEX on omicron.public.volume (
    time_deleted
);

/*
 * Silos
 */

CREATE TYPE omicron.public.authentication_mode AS ENUM (
  'local',
  'saml'
);

CREATE TYPE omicron.public.user_provision_type AS ENUM (
  'api_only',
  'jit'
);

CREATE TABLE omicron.public.silo (
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

CREATE UNIQUE INDEX ON omicron.public.silo (
    name
) WHERE
    time_deleted IS NULL;

/*
 * Silo users
 */
CREATE TABLE omicron.public.silo_user (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,
    external_id TEXT NOT NULL
);

/* This index lets us quickly find users for a given silo. */
CREATE UNIQUE INDEX ON omicron.public.silo_user (
    silo_id,
    external_id
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.silo_user_password_hash (
    silo_user_id UUID NOT NULL,
    hash TEXT NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY(silo_user_id)
);

/*
 * Silo groups
 */

CREATE TABLE omicron.public.silo_group (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    silo_id UUID NOT NULL,
    external_id TEXT NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.silo_group (
    silo_id,
    external_id
) WHERE
    time_deleted IS NULL;

/*
 * Silo group membership
 */

CREATE TABLE omicron.public.silo_group_membership (
    silo_group_id UUID NOT NULL,
    silo_user_id UUID NOT NULL,

    PRIMARY KEY (silo_group_id, silo_user_id)
);

CREATE INDEX ON omicron.public.silo_group_membership (
    silo_user_id,
    silo_group_id
);

/*
 * Silo identity provider list
 */

CREATE TYPE omicron.public.provider_type AS ENUM (
  'saml'
);

CREATE TABLE omicron.public.identity_provider (
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

CREATE INDEX ON omicron.public.identity_provider (
    id,
    silo_id
) WHERE
    time_deleted IS NULL;

CREATE INDEX ON omicron.public.identity_provider (
    name,
    silo_id
) WHERE
    time_deleted IS NULL;

/*
 * Silo SAML identity provider
 */
CREATE TABLE omicron.public.saml_identity_provider (
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

CREATE INDEX ON omicron.public.saml_identity_provider (
    id,
    silo_id
) WHERE
    time_deleted IS NULL;

/*
 * Users' public SSH keys, per RFD 44
 */
CREATE TABLE omicron.public.ssh_key (
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

CREATE UNIQUE INDEX ON omicron.public.ssh_key (
    silo_user_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Projects
 */

CREATE TABLE omicron.public.project (
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

CREATE UNIQUE INDEX ON omicron.public.project (
    silo_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Instances
 */

CREATE TYPE omicron.public.instance_state AS ENUM (
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
CREATE TABLE omicron.public.instance (
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

    /*
     * TODO Would it make sense for the runtime state to live in a separate
     * table?
     */
    /* Runtime state */
    state omicron.public.instance_state NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,
    /*
     * Sled where the VM is currently running, if any.  Note that when we
     * support live migration, there may be multiple sleds associated with
     * this Instance, but only one will be truly active.  Still, consumers of
     * this information should consider whether they also want to know the other
     * sleds involved in the migration.
     */
    active_sled_id UUID,

    /* Identifies the underlying propolis-server backing the instance. */
    active_propolis_id UUID NOT NULL,
    active_propolis_ip INET,

    /* Identifies the target propolis-server during a migration of the instance. */
    target_propolis_id UUID,

    /*
     * Identifies an ongoing migration for this instance.
     */
    migration_id UUID,

    /*
     * A generation number protecting information about the "location" of a
     * running instance: its active server ID, Propolis ID and IP, and migration
     * information. This is used for mutual exclusion (to allow only one
     * migration to proceed at a time) and to coordinate state changes when a
     * migration finishes.
     */
    propolis_generation INT NOT NULL,

    /* Instance configuration */
    ncpus INT NOT NULL,
    memory INT NOT NULL,
    hostname STRING(63) NOT NULL
);

-- Names for instances within a project should be unique
CREATE UNIQUE INDEX ON omicron.public.instance (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

-- Allow looking up instances by server. This is particularly
-- useful for resource accounting within a sled.
CREATE INDEX ON omicron.public.instance (
    active_sled_id
) WHERE
    time_deleted IS NULL;

/*
 * A special view of an instance provided to operators for insights into what's running 
 * on a sled.
 */

CREATE VIEW omicron.public.sled_instance 
AS SELECT
   instance.id,
   instance.name,
   silo.name as silo_name,
   project.name as project_name,
   instance.active_sled_id,
   instance.time_created,
   instance.time_modified,
   instance.migration_id,
   instance.ncpus,
   instance.memory,
   instance.state
FROM
    omicron.public.instance AS instance
    JOIN omicron.public.project AS project ON
            instance.project_id = project.id
    JOIN omicron.public.silo AS silo ON
            project.silo_id = silo.id
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

CREATE TYPE omicron.public.block_size AS ENUM (
  '512',
  '2048',
  '4096'
);

CREATE TABLE omicron.public.disk (
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

CREATE UNIQUE INDEX ON omicron.public.disk (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE INDEX ON omicron.public.disk (
    attach_instance_id
) WHERE
    time_deleted IS NULL AND attach_instance_id IS NOT NULL;

CREATE TABLE omicron.public.image (
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

CREATE VIEW omicron.public.project_image AS
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

CREATE VIEW omicron.public.silo_image AS
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

CREATE UNIQUE INDEX on omicron.public.image (
    silo_id,
    project_id,
    name
) WHERE
    time_deleted is NULL;

CREATE TYPE omicron.public.snapshot_state AS ENUM (
  'creating',
  'ready',
  'faulted',
  'destroyed'
);

CREATE TABLE omicron.public.snapshot (
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

CREATE UNIQUE INDEX ON omicron.public.snapshot (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * Oximeter collector servers.
 */
CREATE TABLE omicron.public.oximeter (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL
);

/*
 * Information about registered metric producers.
 */
CREATE TABLE omicron.public.metric_producer (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,
    interval FLOAT NOT NULL,
    /* TODO: Is this length appropriate? */
    base_route STRING(512) NOT NULL,
    /* Oximeter collector instance to which this metric producer is assigned. */
    oximeter_id UUID NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.metric_producer (
    oximeter_id,
    id
);

/*
 * VPCs and networking primitives
 */


CREATE TABLE omicron.public.vpc (
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

CREATE UNIQUE INDEX ON omicron.public.vpc (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE UNIQUE INDEX ON omicron.public.vpc (
    vni
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.vpc_subnet (
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
CREATE UNIQUE INDEX ON omicron.public.vpc_subnet (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

/* The kind of network interface. */
CREATE TYPE omicron.public.network_interface_kind AS ENUM (
    /* An interface attached to a guest instance. */
    'instance',

    /* An interface attached to a service. */
    'service'
);

CREATE TABLE omicron.public.network_interface (
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
CREATE VIEW omicron.public.instance_network_interface AS
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
CREATE VIEW omicron.public.service_network_interface AS
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
CREATE UNIQUE INDEX ON omicron.public.network_interface (
    subnet_id,
    ip
) WHERE
    time_deleted IS NULL;

/* Ensure we do not assign the same MAC twice within a VPC
 * See RFD174's discussion on the scope of virtual MACs
 */
CREATE UNIQUE INDEX ON omicron.public.network_interface (
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
CREATE UNIQUE INDEX ON omicron.public.network_interface (
    parent_id,
    name,
    kind
)
STORING (vpc_id, subnet_id, is_primary)
WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.vpc_firewall_rule_status AS ENUM (
    'disabled',
    'enabled'
);

CREATE TYPE omicron.public.vpc_firewall_rule_direction AS ENUM (
    'inbound',
    'outbound'
);

CREATE TYPE omicron.public.vpc_firewall_rule_action AS ENUM (
    'allow',
    'deny'
);

CREATE TYPE omicron.public.vpc_firewall_rule_protocol AS ENUM (
    'TCP',
    'UDP',
    'ICMP'
);

CREATE TABLE omicron.public.vpc_firewall_rule (
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

CREATE UNIQUE INDEX ON omicron.public.vpc_firewall_rule (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.vpc_router_kind AS ENUM (
    'system',
    'custom'
);

CREATE TABLE omicron.public.vpc_router (
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

CREATE UNIQUE INDEX ON omicron.public.vpc_router (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.router_route_kind AS ENUM (
    'default',
    'vpc_subnet',
    'vpc_peering',
    'custom'
);

CREATE TABLE omicron.public.router_route (
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

CREATE UNIQUE INDEX ON omicron.public.router_route (
    vpc_router_id,
    name
) WHERE
    time_deleted IS NULL;

/*
 * An IP Pool, a collection of zero or more IP ranges for external IPs.
 */
CREATE TABLE omicron.public.ip_pool (
    /* Resource identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,


    /* Identifies if the IP Pool is dedicated to Control Plane services */
    internal BOOL NOT NULL,

    /* The collection's child-resource generation number */
    rcgen INT8 NOT NULL
);

/*
 * Index ensuring uniqueness of IP Pool names, globally.
 */
CREATE UNIQUE INDEX ON omicron.public.ip_pool (
    name
) WHERE
    time_deleted IS NULL;

/*
 * IP Pools are made up of a set of IP ranges, which are start/stop addresses.
 * Note that these need not be CIDR blocks or well-behaved subnets with a
 * specific netmask.
 */
CREATE TABLE omicron.public.ip_pool_range (
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
CREATE UNIQUE INDEX ON omicron.public.ip_pool_range (
    first_address
)
STORING (last_address)
WHERE time_deleted IS NULL;
CREATE UNIQUE INDEX ON omicron.public.ip_pool_range (
    last_address
)
STORING (first_address)
WHERE time_deleted IS NULL;


/* The kind of external IP address. */
CREATE TYPE omicron.public.ip_kind AS ENUM (
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
CREATE TABLE omicron.public.external_ip (
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
CREATE INDEX ON omicron.public.external_ip (
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
CREATE UNIQUE INDEX ON omicron.public.external_ip (
    ip,
    first_port
)
    WHERE time_deleted IS NULL;

CREATE UNIQUE INDEX ON omicron.public.external_ip (
    parent_id,
    id
)
    WHERE parent_id IS NOT NULL AND time_deleted IS NULL;

/*******************************************************************/

/*
 * Sagas
 */

CREATE TYPE omicron.public.saga_state AS ENUM (
    'running',
    'unwinding',
    'done'
);


CREATE TABLE omicron.public.saga (
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
CREATE UNIQUE INDEX ON omicron.public.saga (
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

CREATE TABLE omicron.public.saga_node_event (
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
CREATE TABLE omicron.public.console_session (
    token STRING(40) PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);

-- to be used for cleaning up old tokens
CREATE INDEX ON omicron.public.console_session (
    time_created
);

-- This index is used to remove sessions for a user that's being deleted.
CREATE INDEX ON omicron.public.console_session (
    silo_user_id
);

/*******************************************************************/

CREATE TYPE omicron.public.update_artifact_kind AS ENUM (
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

CREATE TABLE omicron.public.update_artifact (
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
CREATE INDEX ON omicron.public.update_artifact (
    targets_role_version
);

/*
 * System updates
 */
CREATE TABLE omicron.public.system_update (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- Because the version is unique, it could be the PK, but that would make
    -- this resource different from every other resource for little benefit.

    -- Unique semver version
    version STRING(64) NOT NULL -- TODO: length
);

CREATE UNIQUE INDEX ON omicron.public.system_update (
    version
);

 
CREATE TYPE omicron.public.updateable_component_type AS ENUM (
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
CREATE TABLE omicron.public.component_update (
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
CREATE UNIQUE INDEX ON omicron.public.component_update (
    component_type, version
);

/*
 * Associate system updates with component updates. Not done with a
 * system_update_id field on component_update because the same component update
 * may be part of more than one system update.
 */
CREATE TABLE omicron.public.system_update_component_update (
    system_update_id UUID NOT NULL,
    component_update_id UUID NOT NULL,

    PRIMARY KEY (system_update_id, component_update_id)
);

-- For now, the plan is to treat stopped, failed, completed as sub-cases of
-- "steady" described by a "reason". But reason is not implemented yet.
-- Obviously this could be a boolean, but boolean status fields never stay
-- boolean for long.
CREATE TYPE omicron.public.update_status AS ENUM (
    'updating',
    'steady'
);

/*
 * Updateable components and their update status
 */
CREATE TABLE omicron.public.updateable_component (
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
CREATE UNIQUE INDEX ON omicron.public.updateable_component (
    component_type, device_id
);

CREATE INDEX ON omicron.public.updateable_component (
    system_version
);

/*
 * System updates
 */
CREATE TABLE omicron.public.update_deployment (
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

CREATE INDEX on omicron.public.update_deployment (
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
CREATE TYPE omicron.public.dns_group AS ENUM (
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
CREATE TABLE omicron.public.dns_zone (
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
CREATE UNIQUE INDEX ON omicron.public.dns_zone (
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
CREATE TABLE omicron.public.dns_version (
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
CREATE TABLE omicron.public.dns_name (
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
CREATE UNIQUE INDEX ON omicron.public.dns_name (
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
CREATE TABLE omicron.public.user_builtin (
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

CREATE UNIQUE INDEX ON omicron.public.user_builtin (name);

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
);

/*
 * OAuth 2.0 Device Authorization Grant (RFC 8628)
 */

-- Device authorization requests. These records are short-lived,
-- and removed as soon as a token is granted. This allows us to
-- use the `user_code` as primary key, despite it not having very
-- much entropy.
-- TODO: A background task should remove unused expired records.
CREATE TABLE omicron.public.device_auth_request (
    user_code STRING(20) PRIMARY KEY,
    client_id UUID NOT NULL,
    device_code STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ NOT NULL
);

-- Access tokens granted in response to successful device authorization flows.
CREATE TABLE omicron.public.device_access_token (
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
CREATE UNIQUE INDEX ON omicron.public.device_access_token (
    client_id, device_code
);

-- This index is used to remove tokens for a user that's being deleted.
CREATE INDEX ON omicron.public.device_access_token (
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
CREATE TABLE omicron.public.role_builtin (
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

CREATE TYPE omicron.public.identity_type AS ENUM (
  'user_builtin',
  'silo_user',
  'silo_group'
);

CREATE TABLE omicron.public.role_assignment (
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

CREATE TYPE omicron.public.address_lot_kind AS ENUM (
    'infra',
    'pool'
);

CREATE TABLE omicron.public.address_lot (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    kind omicron.public.address_lot_kind NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.address_lot (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.address_lot_block (
    id UUID PRIMARY KEY,
    address_lot_id UUID NOT NULL,
    first_address INET NOT NULL,
    last_address INET NOT NULL
);

CREATE INDEX ON omicron.public.address_lot_block (
    address_lot_id
);

CREATE TABLE omicron.public.address_lot_rsvd_block (
    id UUID PRIMARY KEY,
    address_lot_id UUID NOT NULL,
    first_address INET NOT NULL,
    last_address INET NOT NULL
);

CREATE INDEX ON omicron.public.address_lot_rsvd_block (
    address_lot_id
);

CREATE TABLE omicron.public.loopback_address (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    address_lot_block_id UUID NOT NULL,
    rsvd_address_lot_block_id UUID NOT NULL,
    rack_id UUID NOT NULL,
    switch_location TEXT NOT NULL,
    address INET NOT NULL
);

/* TODO https://github.com/oxidecomputer/omicron/issues/3001 */

CREATE UNIQUE INDEX ON omicron.public.loopback_address (
    address, rack_id, switch_location
);

CREATE TABLE omicron.public.switch_port (
    id UUID PRIMARY KEY,
    rack_id UUID,
    switch_location TEXT,
    port_name TEXT,
    port_settings_id UUID,

    CONSTRAINT switch_port_rack_locaction_name_unique UNIQUE (
        rack_id, switch_location, port_name
    )
);

/* port settings groups included from port settings objects */
CREATE TABLE omicron.public.switch_port_settings_groups (
    port_settings_id UUID,
    port_settings_group_id UUID,

    PRIMARY KEY (port_settings_id, port_settings_group_id)
);

CREATE TABLE omicron.public.switch_port_settings_group (
    id UUID PRIMARY KEY,
    /* port settings in this group */
    port_settings_id UUID NOT NULL,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX ON omicron.public.switch_port_settings_group (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.switch_port_settings (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX ON omicron.public.switch_port_settings (
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.switch_port_geometry AS ENUM (
    'Qsfp28x1',
    'Qsfp28x2',
    'Sfp28x4'
);

CREATE TABLE omicron.public.switch_port_settings_port_config (
    port_settings_id UUID PRIMARY KEY,
    geometry omicron.public.switch_port_geometry
);

CREATE TABLE omicron.public.switch_port_settings_link_config (
    port_settings_id UUID,
    lldp_service_config_id UUID NOT NULL,
    link_name TEXT,
    mtu INT4,

    PRIMARY KEY (port_settings_id, link_name)
);

CREATE TABLE omicron.public.lldp_service_config (
    id UUID PRIMARY KEY,
    lldp_config_id UUID,
    enabled BOOL NOT NULL
);

CREATE TABLE omicron.public.lldp_config (
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

CREATE UNIQUE INDEX ON omicron.public.lldp_config (
    name
) WHERE
    time_deleted IS NULL;

CREATE TYPE omicron.public.switch_interface_kind AS ENUM (
    'primary',
    'vlan',
    'loopback'
);

CREATE TABLE omicron.public.switch_port_settings_interface_config (
    port_settings_id UUID,
    id UUID PRIMARY KEY,
    interface_name TEXT NOT NULL,
    v6_enabled BOOL NOT NULL,
    kind omicron.public.switch_interface_kind
);

CREATE UNIQUE INDEX ON omicron.public.switch_port_settings_interface_config (
    port_settings_id, interface_name
);

CREATE TABLE omicron.public.switch_vlan_interface_config (
    interface_config_id UUID,
    vid INT4,

    PRIMARY KEY (interface_config_id, vid)
);

CREATE TABLE omicron.public.switch_port_settings_route_config (
    port_settings_id UUID,
    interface_name TEXT,
    dst INET,
    gw INET,
    vid INT4,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, interface_name, dst, gw)
);

CREATE TABLE omicron.public.switch_port_settings_bgp_peer_config (
    port_settings_id UUID,
    bgp_announce_set_id UUID NOT NULL,
    bgp_config_id UUID NOT NULL,
    interface_name TEXT,
    addr INET,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, interface_name, addr)
);

CREATE TABLE omicron.public.bgp_config (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    asn INT8 NOT NULL,
    vrf TEXT
);

CREATE UNIQUE INDEX ON omicron.public.bgp_config (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.bgp_announce_set (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);

CREATE UNIQUE INDEX ON omicron.public.bgp_announce_set (
    name
) WHERE
    time_deleted IS NULL;

CREATE TABLE omicron.public.bgp_announcement (
    announce_set_id UUID,
    address_lot_block_id UUID NOT NULL,
    network INET,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (announce_set_id, network)
);

CREATE TABLE omicron.public.switch_port_settings_address_config (
    port_settings_id UUID,
    address_lot_block_id UUID NOT NULL,
    rsvd_address_lot_block_id UUID NOT NULL,
    address INET,
    interface_name TEXT,

    /* TODO https://github.com/oxidecomputer/omicron/issues/3013 */
    PRIMARY KEY (port_settings_id, address, interface_name)
);


/*******************************************************************/

/*
 * Metadata for the schema itself.  This version number isn't great, as there's
 * nothing to ensure it gets bumped when it should be, but it's a start.
 */

CREATE TABLE omicron.public.db_metadata (
    name  STRING(63) NOT NULL,
    value STRING(1023) NOT NULL
);

INSERT INTO omicron.public.db_metadata (
    name,
    value
) VALUES
    ( 'schema_version', '1.0.0' ),
    ( 'schema_time_created', CAST(NOW() AS STRING) );
