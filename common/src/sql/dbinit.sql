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
 *    The syntax STRING(63) means a Unicode string with at most 63 code points,
 *    not 63 bytes.  In many cases, Nexus itself will validate a string's
 *    byte count or code points, so it's still reasonable to limit ourselves to
 *    powers of two (or powers-of-two-minus-one) to improve storage utilization.
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

    /* The IP address and bound port of the sled agent server. */
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    /* The last address allocated to an Oxide service on this sled. */
    last_used_address INET NOT NULL
);

/* Add an index which lets us look up sleds on a rack */
CREATE INDEX ON omicron.public.sled (
    rack_id
) WHERE time_deleted IS NULL;

/*
 * Services
 */

CREATE TYPE omicron.public.service_kind AS ENUM (
  'internal_dns',
  'nexus',
  'oximeter'
);

CREATE TABLE omicron.public.service (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /* FK into the Sled table */
    sled_id UUID NOT NULL,
    /* The IP address of the service. */
    ip INET NOT NULL,
    /* Indicates the type of service. */
    kind omicron.public.service_kind NOT NULL
);

/* Add an index which lets us look up the services on a sled */
CREATE INDEX ON omicron.public.service (
    sled_id
);

/*
 * ZPools of Storage, attached to Sleds.
 * Typically these are backed by a single physical disk.
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

    /* TODO: Could also store physical disk FK here */

    total_size INT NOT NULL
);

CREATE TYPE omicron.public.dataset_kind AS ENUM (
  'crucible',
  'cockroach',
  'clickhouse'
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
    size_used INT
);

/* Create an index on the size usage for Crucible's allocation */
CREATE INDEX on omicron.public.Dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL AND kind = 'crucible';

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
    data TEXT NOT NULL
);

/*
 * Silos
 */

CREATE TYPE omicron.public.user_provision_type AS ENUM (
  'fixed',
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
    user_provision_type omicron.public.user_provision_type NOT NULL,

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
 * Organizations
 */

CREATE TABLE omicron.public.organization (
    /* Identity metadata */
    id UUID PRIMARY KEY,

    /* FK into Silo table */
    silo_id UUID NOT NULL,

    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.organization (
    silo_id,
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

    /* Which organization this project belongs to */
    organization_id UUID NOT NULL /* foreign key into "Organization" table */
);

CREATE UNIQUE INDEX ON omicron.public.project (
    organization_id,
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
     * Server where the VM is currently running, if any.  Note that when we
     * support live migration, there may be multiple servers associated with
     * this Instance, but only one will be truly active.  Still, consumers of
     * this information should consider whether they also want to know the other
     * servers involved in the migration.
     */
    active_server_id UUID,
    /* Identifies the underlying propolis-server backing the instance. */
    active_propolis_id UUID NOT NULL,
    active_propolis_ip INET,

    /* Identifies the target propolis-server during a migration of the instance. */
    target_propolis_id UUID,

    /*
     * Identifies an ongoing migration for this instance.
     */
    migration_id UUID,

    /* Instance configuration */
    ncpus INT NOT NULL,
    memory INT NOT NULL,
    hostname STRING(63) NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.instance (
    project_id,
    name
) WHERE
    time_deleted IS NULL;


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
    disk_state STRING(15) NOT NULL,
    /*
     * Every Disk may be attaching to, attached to, or detaching from at most
     * one Instance at a time.
     */
    attach_instance_id UUID,
    state_generation INT NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,

    /* Disk configuration */
    size_bytes INT NOT NULL,
    block_size omicron.public.block_size NOT NULL,
    origin_snapshot UUID,
    origin_image UUID
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

    project_id UUID NOT NULL,
    volume_id UUID NOT NULL,

    url STRING(8192),
    version STRING(64),
    digest TEXT,
    block_size omicron.public.block_size NOT NULL,
    size_bytes INT NOT NULL
);

CREATE UNIQUE INDEX on omicron.public.image (
    project_id,
    name
) WHERE
    time_deleted is NULL;

CREATE TABLE omicron.public.global_image (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    volume_id UUID NOT NULL,

    url STRING(8192),
    distribution STRING(64) NOT NULL,
    version STRING(64) NOT NULL,
    digest TEXT,
    block_size omicron.public.block_size NOT NULL,
    size_bytes INT NOT NULL
);

CREATE UNIQUE INDEX on omicron.public.global_image (
    name
) WHERE
    time_deleted is NULL;

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

CREATE TABLE omicron.public.network_interface (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ,

    /* FK into Instance table.
     * Note that interfaces are always attached to a particular instance.
     * IP addresses may be reserved, but this is a different resource.
     */
    instance_id UUID NOT NULL,

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

    ip INET NOT NULL,
    /*
     * Limited to 8 NICs per instance. This value must be kept in sync with
     * `crate::nexus::MAX_NICS_PER_INSTANCE`.
     */
    slot INT2 NOT NULL CHECK (slot >= 0 AND slot < 8),

    /* True if this interface is the primary interface for the instance.
     *
     * The primary interface appears in DNS and its address is used for external
     * connectivity for the instance.
     */
    is_primary BOOL NOT NULL
);

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
 * Index used to verify that an Instance's networking is contained
 * within a single VPC, and that all interfaces are in unique VPC
 * Subnets.
 *
 * This is also used to quickly find the primary interface for an
 * instance, since we store the `is_primary` column. Such queries are
 * mostly used when setting a new primary interface for an instance.
 */
CREATE UNIQUE INDEX ON omicron.public.network_interface (
    instance_id,
    name
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

    /* Optional ID of the project for which this pool is reserved. */
    project_id UUID,

    /*
     * Optional rack ID, indicating this is a reserved pool for internal
     * services on a specific rack.
     * TODO(https://github.com/oxidecomputer/omicron/issues/1276): This
     * should probably point to an AZ or fleet, not a rack.
     */
    rack_id UUID,

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
 * Index ensuring uniqueness of IP pools by rack ID
 */
CREATE UNIQUE INDEX ON omicron.public.ip_pool (
    rack_id
) WHERE
    rack_id IS NOT NULL AND
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
    /* Optional ID of the project for which this range is reserved.
     *
     * NOTE: This denormalizes the tables a bit, since the project_id is
     * duplicated here and in the parent `ip_pool` table. We're allowing this
     * for now, since it reduces the complexity of the already-bad IP allocation
     * query, but we may want to revisit that, and JOIN with the parent table
     * instead.
     */
    project_id UUID,
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

/*
 * Index supporting allocation of IPs out of a Pool reserved for a project.
 */
CREATE INDEX ON omicron.public.ip_pool_range (
    project_id
) WHERE
    time_deleted IS NULL;


/* The kind of external IP address. */
CREATE TYPE omicron.public.ip_kind AS ENUM (
    /* Automatic source NAT provided to all guests by default */
    'snat',

    /*
     * An ephemeral IP is a fixed, known address whose lifetime is the same as
     * the instance to which it is attached.
     */
    'ephemeral',

    /*
     * A floating IP is an independent, named API resource. It is a fixed,
     * known address that can be moved between instances. Its lifetime is not
     * fixed to any instance.
     */
    'floating',

    /*
     * A service IP is an IP address not attached to a project nor an instance.
     * It's intended to be used for internal services.
     */
    'service'
);

/*
 * External IP addresses used for guest instances
 */
CREATE TABLE omicron.public.instance_external_ip (
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

    /* FK to the `project` table. */
    project_id UUID,

    /* FK to the `instance` table. See the constraints below. */
    instance_id UUID,

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
     * Only nullable if this is a floating/service IP, which may exist not
     * attached to any instance.
     */
    CONSTRAINT null_non_fip_instance_id CHECK (
        (kind != 'floating' AND instance_id IS NOT NULL) OR
        (kind = 'floating') OR
        (kind = 'service')
    )
);

/*
 * Index used to support quickly looking up children of the IP Pool range table,
 * when checking for allocated addresses during deletion.
 */
CREATE INDEX ON omicron.public.instance_external_ip (
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
CREATE UNIQUE INDEX ON omicron.public.instance_external_ip (
    ip,
    first_port
)
    WHERE time_deleted IS NULL;

CREATE UNIQUE INDEX ON omicron.public.instance_external_ip (
    instance_id,
    id
)
    WHERE instance_id IS NOT NULL AND time_deleted IS NULL;

/*******************************************************************/

/*
 * Sagas
 */

/*
 * TODO This may eventually have 'paused', 'needs-operator', and 'needs-support'
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

/*******************************************************************/

CREATE TYPE omicron.public.update_artifact_kind AS ENUM (
    'zone'
);

CREATE TABLE omicron.public.update_available_artifact (
    name STRING(40) NOT NULL,
    version INT NOT NULL,
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
CREATE INDEX ON omicron.public.update_available_artifact (
    targets_role_version
);

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
-- TODO-security: expire tokens.
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
