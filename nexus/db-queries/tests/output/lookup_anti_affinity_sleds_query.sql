WITH
  other_instances
    AS (
      SELECT
        group_id, instance_id
      FROM
        anti_affinity_group_instance_membership
      WHERE
        instance_id = $1
    ),
  other_instances_by_policy
    AS (
      SELECT
        policy, instance_id
      FROM
        other_instances
        JOIN anti_affinity_group ON anti_affinity_group.id = other_instances.group_id
      WHERE
        anti_affinity_group.time_deleted IS NULL
    )
SELECT
  policy, sled_id
FROM
  other_instances_by_policy
  JOIN sled_resource ON
      sled_resource.id = other_instances_by_policy.instance_id AND sled_resource.kind = 'instance'
