CREATE UNIQUE INDEX IF NOT EXISTS physical_disk_adoption_request_by_physical_id
ON physical_disk_adoption_request (
   vendor,
   model,
   serial
) WHERE time_deleted IS NULL;
