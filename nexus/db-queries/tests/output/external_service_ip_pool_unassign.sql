WITH
  deleted
    AS (
      DELETE FROM
        external_service_ip_pool
      WHERE
        service = $1
        AND ip_pool_id = $2
        AND CAST(
            IF(
              (SELECT count(1) FROM external_service_ip_pool WHERE service = $3) >= 2,
              'true',
              'unassign-last-pool'
            )
              AS BOOL
          )
      RETURNING
        ip_pool_id
    )
UPDATE
  ip_pool
SET
  rcgen = rcgen + 1
WHERE
  id = $4 AND EXISTS(SELECT 1 FROM deleted)
