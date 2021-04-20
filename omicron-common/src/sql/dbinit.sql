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
GRANT INSERT, SELECT, UPDATE, DELETE ON DATABASE omicron to omicron;

/*
 * Projects
 */

CREATE TABLE omicron.public.Project (
    /* Identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    time_deleted TIMESTAMPTZ
);

/*
 * TODO: Projects eventually need to be linked into organizations, and the
 * "name" is unique within the org.  For now, we just make the name unique among
 * non-deleted projects.
 */
CREATE UNIQUE INDEX ON omicron.public.Project (
    name
) WHERE
    time_deleted IS NULL;

/*
 * Instances
 */

/*
 * TODO We'd like to use this enum for Instance.instance_state.  This doesn't
 * currently work due to cockroachdb/cockroach#57411 /
 * cockroachdb/cockroach#58084.
 */
-- CREATE TYPE omicron.public.InstanceState AS ENUM (
--     'creating',
--     'starting',
--     'running',
--     'stopping',
--     'stopped',
--     'repairing',
--     'failed',
--     'destroyed'
-- );

/*
 * TODO consider how we want to manage multiple sagas operating on the same
 * Instance -- e.g., reboot concurrent with destroy or concurrent reboots or the
 * like.  Or changing # of CPUs or memory size.
 */
CREATE TABLE omicron.public.Instance (
    /* Identity metadata */
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

    /*
     * TODO Would it make sense for the runtime state to live in a separate
     * table?
     */
    /* Runtime state */
    -- instance_state omicron.public.InstanceState NOT NULL, // TODO see above
    instance_state TEXT NOT NULL,
    reboot_in_progress BOOL NOT NULL,
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

    /* Instance configuration */
    ncpus INT NOT NULL,
    memory INT NOT NULL,
    hostname STRING(63) NOT NULL
);

CREATE UNIQUE INDEX ON omicron.public.Instance (
    project_id,
    name
) WHERE
    time_deleted IS NULL;


/*
 * Disks
 */

/*
 * TODO See the note on InstanceState above.
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

CREATE TABLE omicron.public.Disk (
    /* Identity metadata */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    /* Indicates that the object has been deleted */
    /* This is redundant for Disks, but we keep it here for consistency. */
    time_deleted TIMESTAMPTZ,

    /* Every Disk is in exactly one Project at a time. */
    project_id UUID NOT NULL,

    /*
     * TODO Would it make sense for the runtime state to live in a separate
     * table?
     */
    /* Runtime state */
    -- disk_state omicron.public.DiskState NOT NULL, /* TODO see above */
    disk_state STRING(15) NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,
    /*
     * Every Disk may be attaching to, attached to, or detaching from at most
     * one Instance at a time.
     */
    attach_instance_id UUID,

    /* Disk configuration */
    size_bytes INT NOT NULL,
    origin_snapshot UUID
);

CREATE UNIQUE INDEX ON omicron.public.Disk (
    project_id,
    name
) WHERE
    time_deleted IS NULL;

CREATE INDEX ON omicron.public.Disk (
    attach_instance_id
) WHERE
    time_deleted IS NULL AND attach_instance_id IS NOT NULL;


/*
 * Sleds
 */

CREATE TABLE omicron.public.Sled (
    /* Identity metadata -- abbreviated for sleds */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    sled_agent_ip INET
);

/*******************************************************************/

/*
 * Sagas
 */

/*
 * TODO See notes above about cockroachdb/cockroach#57411 /
 * cockroachdb/cockroach#58084.
 * TODO This may eventually have 'paused', 'needs-operator', and 'needs-support'
 */
-- CREATE TYPE omicron.public.SagaState AS ENUM (
--     'running',
--     'unwinding',
--     'done'
-- );


CREATE TABLE omicron.public.Saga (
    /* immutable fields */

    /* unique identifier for this execution */
    id UUID PRIMARY KEY,
    /* unique id of the creator */
    creator UUID NOT NULL,
    /* name of the saga template name being run */
    template_name STRING(127) NOT NULL,
    /* time the saga was started */
    time_created TIMESTAMPTZ NOT NULL,
    /* saga parameters */
    saga_params JSONB NOT NULL,

    /*
     * TODO:
     * - id for current SEC (maybe NULL?)
     * - time of last adoption
     * - previous SEC? previous adoption time?
     * - number of adoptions?
     */
    saga_state STRING(31) NOT NULL, /* see SagaState above */
    current_sec UUID NOT NULL,
    adopt_generation INT NOT NULL,
    adopt_time TIMESTAMPTZ NOT NULL
);

/*
 * For recovery (and probably takeover), we need to be able to list running
 * sagas by SEC.  We need to paginate this list by the id.
 */
CREATE UNIQUE INDEX ON omicron.public.Saga (
    current_sec, id
) WHERE saga_state != 'done';

/*
 * TODO more indexes for Saga?
 * - Debugging and/or reporting: saga_template_name? creator?
 */

/*
 * TODO See notes above about cockroachdb/cockroach#57411 /
 * cockroachdb/cockroach#58084.
 */
-- CREATE TYPE omicron.public.SagaNodeEventType AS ENUM (
--     'started',
--     'succeeded',
--     'failed'
--     'undo_started'
--     'undo_finished'
-- );

CREATE TABLE omicron.public.SagaNodeEvent (
    saga_id UUID NOT NULL,
    node_id INT NOT NULL,
    event_type STRING(31) NOT NULL, /* see SagaNodeEventType above */
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
 * Metadata for the schema itself.  This version number isn't great, as there's
 * nothing to ensure it gets bumped when it should be, but it's a start.
 */

CREATE TABLE omicron.public.DbMetadata (
    name  STRING(63) NOT NULL,
    value STRING(1023) NOT NULL
);

INSERT INTO omicron.public.DbMetadata (
    name,
    value
) VALUES (
    'schema_version',
    '1.0.0'
);
