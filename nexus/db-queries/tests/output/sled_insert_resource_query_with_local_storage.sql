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
  updated_local_storage_disk_records
    AS (
      UPDATE
        disk_type_local_storage
      SET
        local_storage_dataset_allocation_id = CASE disk_id WHEN $9 THEN $10 END
      WHERE
        disk_id IN ($11,)
      RETURNING
        *
    ),
  new_local_storage_allocation_records
    AS (
      INSERT
      INTO
        local_storage_dataset_allocation
      VALUES
        ($12, now(), NULL, $13, $14, $15, $16)
      RETURNING
        *
    ),
  update_rendezvous_tables
    AS (
      UPDATE
        rendezvous_local_storage_dataset
      SET
        size_used = size_used + new_local_storage_allocation_records.dataset_size
      FROM
        new_local_storage_allocation_records
      WHERE
        new_local_storage_allocation_records.local_storage_dataset_id
        = rendezvous_local_storage_dataset.id
        AND rendezvous_local_storage_dataset.time_tombstoned IS NULL
        AND rendezvous_local_storage_dataset.no_provision = false
      RETURNING
        *
    ),
  insert_valid
    AS (
      SELECT
        1
      WHERE
        EXISTS(SELECT 1 FROM sled_has_space)
        AND NOT (EXISTS(SELECT 1 FROM banned_sleds WHERE sled_id = $17))
        AND (
            EXISTS(SELECT 1 FROM required_sleds WHERE sled_id = $18)
            OR NOT EXISTS(SELECT 1 FROM required_sleds)
          )
        AND (
            (
              SELECT
                sum(
                  crucible_dataset.size_used
                  + rendezvous_local_storage_dataset.size_used
                  + new_local_storage_allocation_records.dataset_size
                )
              FROM
                crucible_dataset
                JOIN rendezvous_local_storage_dataset ON
                    crucible_dataset.pool_id = rendezvous_local_storage_dataset.pool_id
                JOIN new_local_storage_allocation_records ON
                    crucible_dataset.pool_id = new_local_storage_allocation_records.pool_id
              WHERE
                (crucible_dataset.size_used IS NOT NULL)
                AND (crucible_dataset.time_deleted IS NULL)
                AND (rendezvous_local_storage_dataset.time_tombstoned IS NULL)
                AND rendezvous_local_storage_dataset.no_provision IS false
                AND crucible_dataset.pool_id = $19
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
                    inv_zpool.id = $20
                  ORDER BY
                    inv_zpool.time_collected DESC
                  LIMIT
                    1
                )
                - (SELECT control_plane_storage_buffer FROM zpool WHERE id = $21)
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
                  zpool.id = $22
              )
            AND (
                SELECT
                  time_tombstoned IS NULL AND no_provision IS false
                FROM
                  rendezvous_local_storage_dataset
                WHERE
                  rendezvous_local_storage_dataset.id = $23
              )
          )
    )
INSERT
INTO
  sled_resource_vmm (id, sled_id, hardware_threads, rss_ram, reservoir_ram, instance_id)
SELECT
  $24, $25, $26, $27, $28, $29
WHERE
  EXISTS(SELECT 1 FROM insert_valid)
