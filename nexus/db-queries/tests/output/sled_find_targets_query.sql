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
  our_a_groups AS (SELECT group_id FROM affinity_group_instance_membership WHERE instance_id = $4),
  other_a_instances
    AS (
      SELECT
        affinity_group_instance_membership.group_id, instance_id
      FROM
        affinity_group_instance_membership
        JOIN our_a_groups ON affinity_group_instance_membership.group_id = our_a_groups.group_id
      WHERE
        instance_id != $5
    ),
  our_direct_aa_groups
    AS (SELECT group_id FROM anti_affinity_group_instance_membership WHERE instance_id = $6),
  other_direct_aa_instances
    AS (
      SELECT
        anti_affinity_group_instance_membership.group_id, instance_id
      FROM
        anti_affinity_group_instance_membership
        JOIN our_direct_aa_groups ON
            anti_affinity_group_instance_membership.group_id = our_direct_aa_groups.group_id
      WHERE
        instance_id != $7
    ),
  our_indirect_aa_groups
    AS (
      SELECT
        anti_affinity_group_id,
        affinity_group_id,
        CASE
        WHEN count(*) OVER (PARTITION BY anti_affinity_group_id) = 1 THEN true
        ELSE false
        END
          AS exactly_one_affinity_group
      FROM
        anti_affinity_group_affinity_membership
      WHERE
        affinity_group_id IN (SELECT group_id FROM our_a_groups)
    ),
  other_indirect_aa_instances_via_instances
    AS (
      SELECT
        anti_affinity_group_id AS group_id, instance_id
      FROM
        anti_affinity_group_instance_membership
        JOIN our_indirect_aa_groups ON
            anti_affinity_group_instance_membership.group_id
            = our_indirect_aa_groups.anti_affinity_group_id
    ),
  other_indirect_aa_instances_via_groups
    AS (
      SELECT
        anti_affinity_group_id AS group_id, instance_id
      FROM
        affinity_group_instance_membership
        JOIN our_indirect_aa_groups ON
            affinity_group_instance_membership.group_id = our_indirect_aa_groups.affinity_group_id
      WHERE
        CASE
        WHEN our_indirect_aa_groups.exactly_one_affinity_group
        THEN affinity_group_instance_membership.group_id NOT IN (SELECT group_id FROM our_a_groups)
        ELSE true
        END
    ),
  other_aa_instances
    AS (
      SELECT * FROM other_direct_aa_instances
      UNION SELECT * FROM other_indirect_aa_instances_via_instances
      UNION SELECT * FROM other_indirect_aa_instances_via_groups
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
    ),
  sleds_with_space
    AS (
      SELECT
        s.sled_id, a.policy AS a_policy, aa.policy AS aa_policy
      FROM
        sled_targets AS s
        LEFT JOIN a_policy_and_sleds AS a ON a.sled_id = s.sled_id
        LEFT JOIN aa_policy_and_sleds AS aa ON aa.sled_id = s.sled_id
    ),
  sleds_without_space
    AS (
      SELECT
        sled_id, policy AS a_policy, NULL AS aa_policy
      FROM
        a_policy_and_sleds
      WHERE
        a_policy_and_sleds.sled_id NOT IN (SELECT sled_id FROM sleds_with_space)
    )
SELECT sled_id, true, a_policy, aa_policy FROM sleds_with_space
UNION SELECT sled_id, false, a_policy, aa_policy FROM sleds_without_space
