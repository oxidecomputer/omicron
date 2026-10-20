SET LOCAL disallow_full_table_scans = off;

WITH
 sled_resource_vmm_update AS (
  SELECT
    sled_resource_vmm.id,
    CAST(
      (CASE
       WHEN sled_resource_vmm.id = instance.active_propolis_id THEN 'active'
       WHEN sled_resource_vmm.id = instance.target_propolis_id THEN 'target'
       ELSE 'tombstoned'
       END) AS omicron.public.sled_resource_vmm_state
    ) AS state
  FROM
    instance
  RIGHT JOIN
    sled_resource_vmm
  ON
    sled_resource_vmm.instance_id = instance.id
 )
UPDATE
 sled_resource_vmm
SET
 state = sled_resource_vmm_update.state
FROM
 sled_resource_vmm_update
WHERE
 sled_resource_vmm.id = sled_resource_vmm_update.id
;
