SET LOCAL disallow_full_table_scans = 'off';
INSERT INTO omicron.public.disk_type_crucible
  SELECT
    id as disk_id,
    volume_id,
    origin_snapshot,
    origin_image,
    pantry_address
  FROM
    omicron.public.disk
;
