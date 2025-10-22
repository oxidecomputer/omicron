CREATE UNIQUE INDEX IF NOT EXISTS
    lookup_scim_client_by_silo_id
ON
    omicron.public.scim_client_bearer_token (silo_id, id)
WHERE
    time_deleted IS NULL;
