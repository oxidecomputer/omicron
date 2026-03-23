-- RPW cleanup of soft-deleted groups
-- Supports: SELECT ... WHERE state = 'deleting' (includes rows with time_deleted set)
-- Without WHERE clause to allow queries on Deleting state regardless of time_deleted
CREATE INDEX IF NOT EXISTS multicast_group_cleanup ON omicron.public.multicast_group (
    state,
    id
);
