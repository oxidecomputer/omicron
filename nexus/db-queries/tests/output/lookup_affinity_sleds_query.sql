WITH
  our_groups AS (SELECT group_id FROM affinity_group_instance_membership WHERE instance_id = $1),
  other_instances
    AS (
      SELECT
        affinity_group_instance_membership.group_id, instance_id
      FROM
        affinity_group_instance_membership
        JOIN our_groups ON affinity_group_instance_membership.group_id = our_groups.group_id
      WHERE
        instance_id != $2
    ),
  other_instances_by_policy
    AS (
      SELECT
        policy, instance_id
      FROM
        other_instances
        JOIN affinity_group ON
            affinity_group.id = other_instances.group_id AND affinity_group.failure_domain = 'sled'
      WHERE
        affinity_group.time_deleted IS NULL
    )
SELECT
  DISTINCT policy, sled_id
FROM
  other_instances_by_policy
  JOIN sled_resource ON
      sled_resource.instance_id = other_instances_by_policy.instance_id
      AND sled_resource.kind = 'instance'
