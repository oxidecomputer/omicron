WITH
  sled_has_space
    AS (
      SELECT
        sled.id AS sled_id
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
        AND NOT (EXISTS(SELECT $9 FROM banned_sleds))
        AND (EXISTS(SELECT $10 FROM required_sleds) OR NOT EXISTS(SELECT 1 FROM required_sleds))
    )
INSERT
INTO
  sled_resource_vmm (id, sled_id, hardware_threads, rss_ram, reservoir_ram, instance_id)
SELECT
  $11, $12, $13, $14, $15, $16
WHERE
  EXISTS(SELECT 1 FROM insert_valid)
