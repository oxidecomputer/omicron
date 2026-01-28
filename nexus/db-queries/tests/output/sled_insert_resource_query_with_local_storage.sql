WITH
  sled_has_space
    AS (
      SELECT
        1
      FROM
        sled LEFT JOIN sled_resource_vmm ON sled_resource_vmm.sled_id = sled.id
      WHERE
        sled.id = $1
        AND sled.time_deleted IS NULL
        AND sled.sled_policy = 'in_service'
        AND sled.sled_state = 'active'
      GROUP BY
        sled.id
      HAVING
        COALESCE(sum(CAST(sled_resource_vmm.hardware_threads AS INT8)), 0) + $2
        <= sled.usable_hardware_threads
        AND COALESCE(sum(CAST(sled_resource_vmm.rss_ram AS INT8)), 0) + $3
          <= sled.usable_physical_ram
        AND COALESCE(sum(CAST(sled_resource_vmm.reservoir_ram AS INT8)), 0) + $4
          <= sled.reservoir_size
    ),
  our_aa_groups
    AS (SELECT group_id FROM anti_affinity_group_instance_membership WHERE instance_id = $5),
  other_aa_instances
    AS (
      SELECT
        anti_affinity_group_instance_membership.group_id, instance_id
      FROM
        anti_affinity_group_instance_membership
        JOIN our_aa_groups ON
            anti_affinity_group_instance_membership.group_id = our_aa_groups.group_id
      WHERE
        instance_id != $6
    ),
  banned_instances
    AS (
      SELECT
        instance_id
      FROM
        other_aa_instances
        JOIN anti_affinity_group ON
            anti_affinity_group.id = other_aa_instances.group_id
            AND anti_affinity_group.failure_domain = 'sled'
            AND anti_affinity_group.policy = 'fail'
      WHERE
        anti_affinity_group.time_deleted IS NULL
    ),
  banned_sleds
    AS (
      SELECT
        DISTINCT sled_id
      FROM
        banned_instances
        JOIN sled_resource_vmm ON sled_resource_vmm.instance_id = banned_instances.instance_id
    ),
  our_a_groups AS (SELECT group_id FROM affinity_group_instance_membership WHERE instance_id = $7),
  other_a_instances
    AS (
      SELECT
        affinity_group_instance_membership.group_id, instance_id
      FROM
        affinity_group_instance_membership
        JOIN our_a_groups ON affinity_group_instance_membership.group_id = our_a_groups.group_id
      WHERE
        instance_id != $8
    ),
  required_instances
    AS (
      SELECT
        policy, instance_id
      FROM
        other_a_instances
        JOIN affinity_group ON
            affinity_group.id = other_a_instances.group_id
            AND affinity_group.failure_domain = 'sled'
            AND affinity_group.policy = 'fail'
      WHERE
        affinity_group.time_deleted IS NULL
    ),
  required_sleds
    AS (
      SELECT
        DISTINCT sled_id
      FROM
        required_instances
        JOIN sled_resource_vmm ON sled_resource_vmm.instance_id = required_instances.instance_id
    ),
  insert_valid
    AS (
      SELECT
        1
      WHERE
        EXISTS(SELECT 1 FROM sled_has_space)
        AND NOT (EXISTS(SELECT 1 FROM banned_sleds WHERE sled_id = $9))
        AND (
            EXISTS(SELECT 1 FROM required_sleds WHERE sled_id = $10)
            OR NOT EXISTS(SELECT 1 FROM required_sleds)
          )
        AND (
            (
              SELECT
                sum(crucible_dataset.size_used + rendezvous_local_storage_dataset.size_used + $11)
              FROM
                crucible_dataset
                JOIN rendezvous_local_storage_dataset ON
                    crucible_dataset.pool_id = rendezvous_local_storage_dataset.pool_id
              WHERE
                (crucible_dataset.size_used IS NOT NULL)
                AND (crucible_dataset.time_deleted IS NULL)
                AND (rendezvous_local_storage_dataset.time_tombstoned IS NULL)
                AND crucible_dataset.pool_id = $12
              GROUP BY
                crucible_dataset.pool_id
            )
            < (
                (
                  SELECT
                    total_size
                  FROM
                    inv_zpool
                  WHERE
                    inv_zpool.id = $13
                  ORDER BY
                    inv_zpool.time_collected DESC
                  LIMIT
                    1
                )
                - (SELECT control_plane_storage_buffer FROM zpool WHERE id = $14)
              )
            AND (
                SELECT
                  sled.sled_policy = 'in_service'
                  AND sled.sled_state = 'active'
                  AND physical_disk.disk_policy = 'in_service'
                  AND physical_disk.disk_state = 'active'
                FROM
                  zpool
                  JOIN sled ON zpool.sled_id = sled.id
                  JOIN physical_disk ON zpool.physical_disk_id = physical_disk.id
                WHERE
                  zpool.id = $15
              )
            AND (
                SELECT
                  time_tombstoned IS NULL AND no_provision IS false
                FROM
                  rendezvous_local_storage_dataset
                WHERE
                  rendezvous_local_storage_dataset.id = $16
              )
            AND (
                SELECT
                  sum(crucible_dataset.size_used + rendezvous_local_storage_dataset.size_used + $17)
                FROM
                  crucible_dataset
                  JOIN rendezvous_local_storage_dataset ON
                      crucible_dataset.pool_id = rendezvous_local_storage_dataset.pool_id
                WHERE
                  (crucible_dataset.size_used IS NOT NULL)
                  AND (crucible_dataset.time_deleted IS NULL)
                  AND (rendezvous_local_storage_dataset.time_tombstoned IS NULL)
                  AND crucible_dataset.pool_id = $18
                GROUP BY
                  crucible_dataset.pool_id
              )
              < (
                  (
                    SELECT
                      total_size
                    FROM
                      inv_zpool
                    WHERE
                      inv_zpool.id = $19
                    ORDER BY
                      inv_zpool.time_collected DESC
                    LIMIT
                      1
                  )
                  - (SELECT control_plane_storage_buffer FROM zpool WHERE id = $20)
                )
            AND (
                SELECT
                  sled.sled_policy = 'in_service'
                  AND sled.sled_state = 'active'
                  AND physical_disk.disk_policy = 'in_service'
                  AND physical_disk.disk_state = 'active'
                FROM
                  zpool
                  JOIN sled ON zpool.sled_id = sled.id
                  JOIN physical_disk ON zpool.physical_disk_id = physical_disk.id
                WHERE
                  zpool.id = $21
              )
            AND (
                SELECT
                  time_tombstoned IS NULL AND no_provision IS false
                FROM
                  rendezvous_local_storage_dataset
                WHERE
                  rendezvous_local_storage_dataset.id = $22
              )
          )
    ),
  updated_local_storage_disk_records
    AS (
      UPDATE
        disk_type_local_storage
      SET
        local_storage_dataset_allocation_id = CASE disk_id WHEN $23 THEN $24 WHEN $25 THEN $26 END
      WHERE
        disk_id IN ($27, $28) AND EXISTS(SELECT 1 FROM insert_valid)
      RETURNING
        *
    ),
  new_local_storage_allocation_records_0
    AS (
      INSERT
      INTO
        local_storage_dataset_allocation
          (id, time_created, time_deleted, local_storage_dataset_id, pool_id, sled_id, dataset_size)
      SELECT
        $29, now(), NULL, $30, $31, $32, $33
      WHERE
        EXISTS(SELECT 1 FROM insert_valid)
      RETURNING
        *
    ),
  new_local_storage_allocation_records_1
    AS (
      INSERT
      INTO
        local_storage_dataset_allocation
          (id, time_created, time_deleted, local_storage_dataset_id, pool_id, sled_id, dataset_size)
      SELECT
        $34, now(), NULL, $35, $36, $37, $38
      WHERE
        EXISTS(SELECT 1 FROM insert_valid)
      RETURNING
        *
    ),
  update_rendezvous_tables
    AS (
      UPDATE
        rendezvous_local_storage_dataset
      SET
        size_used = size_used + CASE pool_id WHEN $39 THEN $40 WHEN $41 THEN $42 END
      WHERE
        pool_id IN ($43, $44) AND time_tombstoned IS NULL AND EXISTS(SELECT 1 FROM insert_valid)
      RETURNING
        *
    )
INSERT
INTO
  sled_resource_vmm (id, sled_id, hardware_threads, rss_ram, reservoir_ram, instance_id)
SELECT
  $45, $46, $47, $48, $49, $50
WHERE
  EXISTS(SELECT 1 FROM insert_valid)
