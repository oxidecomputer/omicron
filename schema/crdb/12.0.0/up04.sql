/*
 * Recreate index to support looking up a producer by its assigned oximeter
 * collector ID.
 */
CREATE UNIQUE INDEX IF NOT EXISTS lookup_producer_by_oximeter ON omicron.public.metric_producer (
    oximeter_id,
    id
);
