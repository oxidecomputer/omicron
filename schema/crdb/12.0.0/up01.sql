/*
 * Drop the entire metric producer assignment table.
 *
 * Programs wishing to produce metrics need to register with Nexus. That creates
 * an assignment of the producer to a collector, which is recorded in this
 * table. That registration is idempotent, and every _current_ producer will
 * register when it restarts. For example, `dpd` includes a task that registers
 * with Nexus, so each time it (re)starts, that registration will happen.
 *
 * With that in mind, dropping this table is safe, because as of today, all
 * software updates reuqire that the whole control plane be offline. We know
 * that these entries will be recreated shortly, as the services registering
 * producers are restarted.
 *
 * The current metric producers are:
 *
 * - `dpd`
 * - Each `nexus` instance
 * - Each `sled-agent` instance
 * - The Propolis server for each guest Instance
 *
 * Another reason we're dropping the table is because we will add a new column,
 * `kind`, in a following update file, but we don't have a good way to backfill
 * that value for existing rows. We also don't need to, because these services
 * will soon reregister, and provide us with a value.
 */
DROP TABLE IF EXISTS omicron.public.metric_producer;
