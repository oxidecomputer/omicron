CREATE VIEW IF NOT EXISTS omicron.public.nat_changes
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
  FROM omicron.public.nat_entry
  WHERE version_removed IS NULL
  UNION
  SELECT
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    version_removed AS version,
    (version_removed IS NOT NULL) as deleted
  FROM omicron.public.nat_entry
  WHERE version_removed IS NOT NULL
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
