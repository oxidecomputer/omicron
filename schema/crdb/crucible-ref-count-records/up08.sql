/* Construct a UUIDv4 deterministically from the existing data in the region
   snapshot records to populate the volume resource usage records */
INSERT INTO volume_resource_usage (
  SELECT
    gen_random_uuid() AS usage_id,
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
    volume.data like ('%' || region_snapshot.snapshot_addr || '%')
  LEFT JOIN
    volume_resource_usage
  ON
    volume_resource_usage.volume_id = volume.id AND
    volume_resource_usage.usage_type = 'region_snapshot' AND
    volume_resource_usage.region_id IS NULL AND
    volume_resource_usage.region_snapshot_region_id = region_snapshot.region_id AND
    volume_resource_usage.region_snapshot_dataset_id = region_snapshot.dataset_id AND
    volume_resource_usage.region_snapshot_snapshot_id = region_snapshot.snapshot_id
  WHERE
    volume.time_deleted is NULL AND
    region_snapshot.deleting = false AND
    volume_resource_usage.usage_id IS NULL
);
