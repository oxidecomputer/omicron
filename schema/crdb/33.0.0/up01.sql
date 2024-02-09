/**
 * A view of the ipv4 nat change history
 * used to summarize changes for external viewing
 */
CREATE VIEW IF NOT EXISTS omicron.public.ipv4_nat_changes
AS
WITH interleaved_versions AS (
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    version_added AS version,
    (version_removed IS NOT NULL) as deleted
  FROM ipv4_nat_entry
  WHERE version_removed IS NULL

  UNION

  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    version_added AS version,
    (version_removed IS NOT NULL) as deleted
  FROM ipv4_nat_entry WHERE version_removed IS NOT NULL
)
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
