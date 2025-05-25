INSERT INTO omicron.public.silo_settings (
  silo_id,
  time_created,
  time_modified,
  device_token_max_ttl_seconds
)
SELECT id, NOW(), NOW(), NULL
FROM omicron.public.silo
WHERE time_deleted IS NULL;
