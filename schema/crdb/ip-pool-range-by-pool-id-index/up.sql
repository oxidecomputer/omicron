-- Index to support efficient lookups of ip_pool_range rows by ip_pool_id.
--
-- This index benefits several important queries:
-- - ip_pool_fetch_containing_address: finds pools containing a specific IP
-- - ip_pool_delete: checks for remaining ranges before pool deletion
-- - ip_pool_list_ranges_batched_on_connection: lists all ranges in a pool
-- - ip_pools_fetch_ssm_multicast_pool / ip_pools_fetch_asm_multicast_pool:
--   joins ip_pool_range to ip_pool for multicast pool lookups
--
-- The STORING clause includes first_address and last_address because these
-- columns are commonly filtered or selected alongside ip_pool_id, allowing
-- the query to be satisfied entirely from the index without an additional
-- lookup to the primary table.
CREATE INDEX IF NOT EXISTS ip_pool_range_by_pool_id ON omicron.public.ip_pool_range (
    ip_pool_id
) STORING (first_address, last_address)
WHERE time_deleted IS NULL;
