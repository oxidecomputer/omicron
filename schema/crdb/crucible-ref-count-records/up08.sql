INSERT INTO volume_resource_usage (
  SELECT
    (
      OVERLAY(
       OVERLAY(
        MD5(volume.id::TEXT || dataset_id::TEXT || region_id::TEXT || snapshot_id::TEXT || snapshot_addr || volume_references::TEXT)
        PLACING '4' from 13
       )
       PLACING TO_HEX(volume_references) from 17
      )::uuid
    ) as usage_id,
    volume.id AS volume_id,
    'region_snapshot' AS usage_type,
    NULL AS region_id,
    region_snapshot.dataset_id AS region_snapshot_dataset_id,
    region_snapshot.region_id AS region_snapshot_region_id,
    region_snapshot.snapshot_id AS region_snapshot_snapshot_id
  FROM
    volume
  JOIN
    region_snapshot
  ON
    volume.data like '%' || region_snapshot.snapshot_addr || '%'
  WHERE
    volume.time_deleted is NULL AND
    region_snapshot.deleting = false
)
ON CONFLICT (usage_id) DO NOTHING
;
