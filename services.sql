/*
 *
 * TAKEN FROM DBINIT.SQL
 *
 */

/* dbwipe.sql */
CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;
ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM omicron;
DROP DATABASE IF EXISTS omicron;
DROP USER IF EXISTS omicron;

/* dbinit.sql */
CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;
ALTER DEFAULT PRIVILEGES GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES to omicron;

set disallow_full_table_scans = on;
set large_full_scan_rows = 0;

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

/* Add an index which lets us look up the sleds on a rack */
CREATE INDEX ON omicron.public.sled (
    rack_id
) WHERE
    time_deleted IS NULL;

CREATE INDEX ON omicron.public.sled (
    id
) WHERE
    time_deleted IS NULL;

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
    sled_id,
    kind
);

/* Add an index which lets us look up services of a particular kind on a sled */
CREATE INDEX ON omicron.public.service (
    kind
);

/*
 * Additional context for services of "kind = nexus"
 * This table should be treated as an optional extension
 * of the service table itself.
 */
CREATE TABLE omicron.public.nexus_service (
    id UUID PRIMARY KEY,

    /* FK to the service table */
    service_id UUID NOT NULL,
    /* FK to the instance_external_ip table */
    external_ip_id UUID NOT NULL,
    /* FK to the nexus_certificate table */
    certificate_id UUID NOT NULL
);

/*
 * Information about x509 certificates used to serve Nexus' external interface.
 * These certificates may be used by multiple instantiations of the Nexus
 * service simultaneously.
 */
CREATE TABLE omicron.public.nexus_certificate (
    id UUID PRIMARY KEY,
    public_cert BYTES NOT NULL,
    private_key BYTES NOT NULL
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

    /* An upper bound on the amount of space that is allowed to be in-use */
    quota INT NOT NULL,
    reservation INT NOT NULL,

    /* An upper bound on the amount of space that might be in-use */
    size_used INT,

    /* A quota smaller than a reservation would reserve unusable space */
    CONSTRAINT reservation_less_than_or_equal_to_quota CHECK (
      reservation <= quota
    ),

    /* Crucible must make use of 'size_used'; other datasets manage their own storage */
    CONSTRAINT size_used_column_set_for_crucible CHECK (
      (kind != 'crucible') OR
      (kind = 'crucible' AND size_used IS NOT NULL)
    ),

    /* Validate that the size usage is less than the quota */
    CONSTRAINT size_used_less_than_or_equal_to_quota CHECK (
      (size_used IS NULL) OR
      (size_used IS NOT NULL AND size_used <= quota)
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

-- TODO: Obviously, there's more stuff here. But this is a proxy.
CREATE TABLE omicron.public.external_ip (
    id UUID PRIMARY KEY
);


/*
 *
 * TEST DATA
 *
 */

-- Add a rack
INSERT INTO omicron.public.rack (id, time_created, time_modified, initialized, tuf_base_url) VALUES
  (
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    TRUE,
    NULL
  );

-- Add some sleds (aaaa / bbbb are gimlets, cccc is scrimlet)
INSERT INTO omicron.public.sled (id, time_created, time_modified, time_deleted, rcgen, rack_id, is_scrimlet, ip, port, last_used_address) VALUES
  (
    '22222222-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    false,
    '127.0.0.1',
    0,
    '127.0.0.1'
  ),
  (
    '22222222-bbbb-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    false,
    '127.0.0.1',
    0,
    '127.0.100.1'
  ),
  (
    '22222222-cccc-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    true,
    '127.0.0.1',
    0,
    '127.0.200.1'
  );

INSERT INTO omicron.public.service (id, time_created, time_modified, sled_id, ip, kind) VALUES
  (
    '33333333-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    '22222222-aaaa-407e-aa8d-602ed78f38be',
    '127.0.0.1',
    'nexus'
  );

/*
 * CTE: Allocate a particular service within a rack.
 *
 * Inputs: Rack ID, service type, desired count
 */

WITH
  -- Find all allocation targets.
  -- This includes sleds which may already be running the service,
  -- and sleds which could run the service in the future.
  sled_allocation_pool AS (
    SELECT
      omicron.public.sled.id
    FROM
      omicron.public.sled
    WHERE
      omicron.public.sled.time_deleted IS NULL AND
      -- XXX: Constraints can be user-supplied?
      omicron.public.sled.rack_id = '11111111-aaaa-407e-aa8d-602ed78f38be'
  ),

  -- Get all services which already have been allocated from this pool.
  previously_allocated_services AS (
    SELECT
      omicron.public.service.id,
      omicron.public.service.time_created,
      omicron.public.service.time_modified,
      omicron.public.service.sled_id,
      omicron.public.service.ip,
      omicron.public.service.kind
    FROM
      omicron.public.service
    WHERE
      -- XXX: 'nexus' is the name of this particular service
      omicron.public.service.kind = 'nexus' AND
      omicron.public.service.sled_id IN (SELECT id FROM sled_allocation_pool)
  ),

  -- Calculate how many services we already have
  old_service_count AS (
    SELECT COUNT(1) FROM previously_allocated_services
  ),
  -- Calculate the number of new services we need
  new_service_count AS (
    -- XXX: 3 is the user-supplied redundancy
    SELECT (greatest(3, (SELECT old_service_count.count FROM old_service_count))
      - (SELECT old_service_count.count FROM old_service_count))
  ),

  -- Get allocation candidates from the pool, as long as they don't already
  -- have the service.
  candidate_sleds AS (
    SELECT
      sled_allocation_pool.id
    FROM
      sled_allocation_pool
    WHERE
      sled_allocation_pool.id NOT IN (SELECT sled_id FROM previously_allocated_services)
    LIMIT (SELECT * FROM new_service_count)
  ),

  -- Allocate an internal IP address for the service
  new_internal_ips AS (
    UPDATE omicron.public.sled
    SET
      last_used_address = last_used_address + 1
    WHERE
      omicron.public.sled.id in (SELECT id from candidate_sleds)
    RETURNING
      omicron.public.sled.id as sled_id,
      omicron.public.sled.last_used_address as ip
  ),

  -- TODO: External IPs???

  -- TODO: This fails; data-modifying statements must be at a top level.
--  new_external_ips AS (
--    WITH
--    new_ips AS (
--      INSERT INTO omicron.public.external_ip (id) VALUES (
--        gen_random_uuid()
--      )
--      RETURNING *
--    )
--    SELECT * FROM (SELECT * FROM new_ips)
--  ),

  -- Construct the services we want to insert
  candidate_services AS (
    SELECT
      gen_random_uuid() as id,
      now() as time_created,
      now() as time_modified,
      candidate_sleds.id as sled_id,
      new_internal_ips.ip as ip,
      -- XXX service type
      CAST('nexus' AS omicron.public.service_kind) as kind
    FROM
      candidate_sleds
    INNER JOIN
      new_internal_ips
    ON
      candidate_sleds.id = new_internal_ips.sled_id
  ),

  inserted_services AS (
    INSERT INTO omicron.public.service
      (
        -- XXX: "SELECT *" isn't currently possible with Diesel...
        -- ... but it *COULD* be, when the source is a CTE Query!
        SELECT *
        FROM candidate_services
      )
    RETURNING *
  )
SELECT * FROM
  (
    SELECT
      -- XXX: Do we care about the new/not new distinction?
      FALSE as new,
      *
    FROM previously_allocated_services
    UNION
    SELECT
      TRUE as new,
      *
    FROM inserted_services
  );

set disallow_full_table_scans = off;

-- SELECT * FROM omicron.public.Sled;
