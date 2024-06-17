-- A table of the states of current migrations.
CREATE TABLE IF NOT EXISTS omicron.public.migration (
    id UUID PRIMARY KEY,

    /* The ID of the instance that was migrated */
    instance_id UUID NOT NULL,

    /* The time this migration record was created. */
    time_created TIMESTAMPTZ NOT NULL,

    /* The time this migration record was deleted. */
    time_deleted TIMESTAMPTZ,

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
