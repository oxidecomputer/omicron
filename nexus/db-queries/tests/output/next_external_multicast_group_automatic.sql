WITH
  next_external_multicast_group
    AS (
      SELECT
        $1 AS id,
        $2 AS name,
        $3 AS description,
        $4 AS time_created,
        $5 AS time_modified,
        $6 AS time_deleted,
        $7 AS vni,
        ip_pool_id,
        ip_pool_range_id,
        candidate_ip AS multicast_ip,
        $8 AS underlay_group_id,
        $9::STRING || ':' || candidate_ip::STRING AS tag,
        $10 AS state,
        nextval('omicron.public.multicast_group_version') AS version_added,
        $11 AS version_removed,
        $12 AS underlay_salt
      FROM
        (
          SELECT
            ip_pool_id,
            id AS ip_pool_range_id,
            first_address + generate_series(0, last_address - first_address) AS candidate_ip
          FROM
            ip_pool_range
          WHERE
            ip_pool_id = $13
            AND time_deleted IS NULL
            AND (first_address << '224.0.0.0/4'::INET OR first_address << 'ff00::/8'::INET)
        )
        LEFT JOIN multicast_group ON multicast_ip = candidate_ip AND time_deleted IS NULL
      WHERE
        candidate_ip IS NOT NULL AND multicast_ip IS NULL
      LIMIT
        1
    ),
  previously_allocated_group
    AS (SELECT * FROM multicast_group WHERE id = $14 AND time_deleted IS NULL),
  multicast_group
    AS (
      INSERT
      INTO
        multicast_group
          (
            id,
            name,
            description,
            time_created,
            time_modified,
            time_deleted,
            vni,
            ip_pool_id,
            ip_pool_range_id,
            multicast_ip,
            underlay_group_id,
            tag,
            state,
            version_added,
            version_removed,
            underlay_salt
          )
      SELECT
        id,
        name,
        description,
        time_created,
        time_modified,
        time_deleted,
        vni,
        ip_pool_id,
        ip_pool_range_id,
        multicast_ip,
        underlay_group_id,
        tag,
        state,
        version_added,
        version_removed,
        underlay_salt
      FROM
        next_external_multicast_group
      WHERE
        NOT EXISTS(SELECT 1 FROM previously_allocated_group)
      RETURNING
        id,
        name,
        description,
        time_created,
        time_modified,
        time_deleted,
        vni,
        ip_pool_id,
        ip_pool_range_id,
        multicast_ip,
        underlay_group_id,
        tag,
        state,
        version_added,
        version_removed,
        underlay_salt
    ),
  updated_pool_range
    AS (
      UPDATE
        ip_pool_range
      SET
        time_modified = $15, rcgen = rcgen + 1
      WHERE
        id = (SELECT ip_pool_range_id FROM next_external_multicast_group) AND time_deleted IS NULL
      RETURNING
        id
    )
SELECT
  id,
  name,
  description,
  time_created,
  time_modified,
  time_deleted,
  vni,
  ip_pool_id,
  ip_pool_range_id,
  multicast_ip,
  underlay_group_id,
  tag,
  state,
  version_added,
  version_removed,
  underlay_salt
FROM
  previously_allocated_group
UNION ALL
  SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    vni,
    ip_pool_id,
    ip_pool_range_id,
    multicast_ip,
    underlay_group_id,
    tag,
    state,
    version_added,
    version_removed,
    underlay_salt
  FROM
    multicast_group
