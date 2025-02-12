WITH
  sled_targets
    AS (
      SELECT
        sled.id AS sled_id
      FROM
        sled LEFT JOIN sled_resource_vmm ON sled_resource_vmm.sled_id = sled.id
      WHERE
        sled.time_deleted IS NULL AND sled.sled_policy = 'in_service' AND sled.sled_state = 'active'
      GROUP BY
        sled.id
      HAVING
        COALESCE(sum(CAST(sled_resource_vmm.hardware_threads AS INT8)), 0) + $1
        <= sled.usable_hardware_threads
        AND COALESCE(sum(CAST(sled_resource_vmm.rss_ram AS INT8)), 0) + $2
          <= sled.usable_physical_ram
        AND COALESCE(sum(CAST(sled_resource_vmm.reservoir_ram AS INT8)), 0) + $3
          <= sled.reservoir_size
    ),
  our_aa_groups
    AS (SELECT group_id FROM anti_affinity_group_instance_membership WHERE instance_id = $4),
  other_aa_instances
    AS (
      SELECT
        anti_affinity_group_instance_membership.group_id, instance_id
      FROM
        anti_affinity_group_instance_membership
        JOIN our_aa_groups ON
            anti_affinity_group_instance_membership.group_id = our_aa_groups.group_id
      WHERE
        instance_id != $5
    ),
  other_aa_instances_by_policy
    AS (
      SELECT
        policy, instance_id
      FROM
        other_aa_instances
        JOIN anti_affinity_group ON
            anti_affinity_group.id = other_aa_instances.group_id
            AND anti_affinity_group.failure_domain = 'sled'
      WHERE
        anti_affinity_group.time_deleted IS NULL
    ),
  aa_policy_and_sleds
    AS (
      SELECT
        DISTINCT policy, sled_id
      FROM
        other_aa_instances_by_policy
        JOIN sled_resource_vmm ON
            sled_resource_vmm.instance_id = other_aa_instances_by_policy.instance_id
    ),
  our_a_groups AS (SELECT group_id FROM affinity_group_instance_membership WHERE instance_id = $6),
  other_a_instances
    AS (
      SELECT
        affinity_group_instance_membership.group_id, instance_id
      FROM
        affinity_group_instance_membership
        JOIN our_a_groups ON affinity_group_instance_membership.group_id = our_a_groups.group_id
      WHERE
        instance_id != $7
    ),
  other_a_instances_by_policy
    AS (
      SELECT
        policy, instance_id
      FROM
        other_a_instances
        JOIN affinity_group ON
            affinity_group.id = other_a_instances.group_id
            AND affinity_group.failure_domain = 'sled'
      WHERE
        affinity_group.time_deleted IS NULL
    ),
  a_policy_and_sleds
    AS (
      SELECT
        DISTINCT policy, sled_id
      FROM
        other_a_instances_by_policy
        JOIN sled_resource_vmm ON
            sled_resource_vmm.instance_id = other_a_instances_by_policy.instance_id
    )
SELECT
  a_policy_and_sleds.policy, aa_policy_and_sleds.policy, sled_targets.sled_id
FROM
  (sled_targets LEFT JOIN a_policy_and_sleds ON a_policy_and_sleds.sled_id = sled_targets.sled_id)
  LEFT JOIN aa_policy_and_sleds ON aa_policy_and_sleds.sled_id = sled_targets.sled_id
