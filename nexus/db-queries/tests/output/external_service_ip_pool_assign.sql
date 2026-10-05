WITH
  bumped
    AS (
      UPDATE
        ip_pool
      SET
        rcgen = rcgen + 1
      WHERE
        id = $1
        AND time_deleted IS NULL
        AND CAST(IF(assignment = $2, 'true', 'not-a-system-services-pool') AS BOOL)
      RETURNING
        id
    )
INSERT
INTO
  external_service_ip_pool (service, ip_pool_id)
SELECT
  $3, id
FROM
  bumped
