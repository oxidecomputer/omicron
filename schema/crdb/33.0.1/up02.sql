/*
 * A view of the ipv4 nat change history
 * used to summarize changes for external viewing
 */
CREATE VIEW IF NOT EXISTS omicron.public.ipv4_nat_changes
AS
-- Subquery:
-- We need to be able to order partial changesets. ORDER BY on separate columns
-- will not accomplish this, so we'll do this by interleaving version_added
-- and version_removed (version_removed taking priority if NOT NULL) and then sorting
-- on the appropriate version numbers at call time.
WITH interleaved_versions AS (
  -- fetch all active NAT entries (entries that have not been soft deleted)
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    -- rename version_added to version
    version_added AS version,
    -- create a new virtual column, boolean value representing whether or not
    -- the record has been soft deleted
    (version_removed IS NOT NULL) as deleted
  FROM omicron.public.ipv4_nat_entry
  WHERE version_removed IS NULL

  -- combine the datasets, unifying the version_added and version_removed
  -- columns to a single `version` column so we can interleave and sort the entries
  UNION

  -- fetch all inactive NAT entries (entries that have been soft deleted)
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    -- rename version_removed to version
    version_removed AS version,
    -- create a new virtual column, boolean value representing whether or not
    -- the record has been soft deleted
    (version_removed IS NOT NULL) as deleted
  FROM omicron.public.ipv4_nat_entry
  WHERE version_removed IS NOT NULL
)
-- this is our new "table"
-- here we select the columns from the subquery defined above
SELECT
  external_address,
  first_port,
  last_port,
  sled_address,
  vni,
  mac,
  version,
  deleted
FROM interleaved_versions;
