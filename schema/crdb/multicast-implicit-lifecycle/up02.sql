-- Drop unused index (no queries filter by state + ip_pool_id)
DROP INDEX IF EXISTS omicron.public.multicast_group_reconciler_query;
