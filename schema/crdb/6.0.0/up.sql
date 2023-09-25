/*
 * This update turns VMM processes into first-class objects in the Omicron data
 * model. Instead of storing an instance's runtime state entirely in the
 * instance table, Nexus stores per-Propolis state and uses the active Propolis
 * ID field of an instance's record to determine which VMM (if any) holds the
 * instance's current state. This makes it much easier for Nexus to reason about
 * the lifecycles of Propolis jobs and their resource requirements (presently
 * Nexus must reason about when a Propolis is gone from changes to the active
 * Propolis ID in an instance record; now it is very obvious when a specific
 * Propolis process has exited).
 *
 * In this scheme:
 * - Sled assignments and Propolis server IPs are now tracked per-VMM.
 * - An instance may not have an active VMM at all; in that case it uses its own
 *   `state` column to report its state.
 * - The instance's two generation numbers (one for the Propolis ID and one for
 *   the reported instance state) are once again collapsed into a single
 *   generation number. The initial value of this number does not matter (since
 *   the upgrade is offline, there are by definition no users who are currently
 *   using its value to synchronize), so just adopt whatever value the existing
 *   `state_generation` column happens to have.
 *
 * To start off, drop the instance-by-sled index, since there's no longer a sled
 * ID in the instance table.
 */

DROP INDEX IF EXISTS lookup_instance_by_sled;

/*
 * The sled_instance view cannot be modified in place because it depends on the
 * VMM table. It would be nice to define the VMM table and then alter the
 * sled_instance table, but there's no way to express this correctly in the
 * clean-slate DB initialization SQL (dbinit.sql) because it requires inserting
 * a table into the middle of an existing sequence of table definitions. (See
 * the README for more on why this causes problems.) Instead, delete the
 * `sled_instance` view, then add the VMM table, then add the view back and
 * leave it to `dbinit.sql` to re-create the resulting object ordering when
 * creating a database from a clean slate.
 */

DROP VIEW IF EXISTS omicron.public.sled_instance;

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

/*
 * Now that the sled_instance view is up-to-date, drop columns from the instance
 * table that are no longer needed. This needs to be done after altering the
 * view because it's illegal to drop columns that a view depends on.
 */

ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS active_sled_id;
ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS active_propolis_ip;
ALTER TABLE omicron.public.instance DROP COLUMN IF EXISTS propolis_generation;

/*
 * Make instances' active Propolis IDs nullable. This is an offline update, so
 * when the system comes back up, there are no active VMMs and all instances'
 * active Propolis IDs should be cleared. This guileless approach gets planned
 * as a full table scan, which is disallowed by default, so temporarily allow
 * such a scan to allow the update to proceed.
 */

ALTER TABLE omicron.public.instance ALTER COLUMN active_propolis_id DROP NOT NULL;

set disallow_full_table_scans = off;
UPDATE omicron.public.instance SET active_propolis_id = NULL;
set disallow_full_table_scans = on;
