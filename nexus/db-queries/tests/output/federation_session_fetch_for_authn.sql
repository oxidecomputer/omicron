SELECT
  federation_session.id,
  federation_session.time_created,
  federation_session.time_last_used,
  federation_session.time_expires,
  federation_session.trust_policy_id,
  federation_session.trust_policy_revision,
  federation_session.jwt_claims,
  federation_session.audit_log_id,
  federation_session.token,
  federation_trust_policy.silo_id
FROM
  (
    (
      federation_session
      INNER JOIN federation_trust_policy ON
          federation_trust_policy.id = federation_session.trust_policy_id
    )
    INNER JOIN federation_identity_provider ON
        federation_identity_provider.id = federation_trust_policy.idp_id
        AND federation_identity_provider.silo_id = federation_trust_policy.silo_id
  )
  INNER JOIN silo ON silo.id = federation_trust_policy.silo_id
WHERE
  (
    (
      (
        (federation_session.token = $1 AND federation_session.time_expires > $2)
        AND federation_session.trust_policy_revision = federation_trust_policy.revision
      )
      AND (federation_trust_policy.time_deleted IS NULL)
    )
    AND (federation_identity_provider.time_deleted IS NULL)
  )
  AND (silo.time_deleted IS NULL)
LIMIT
  $3
