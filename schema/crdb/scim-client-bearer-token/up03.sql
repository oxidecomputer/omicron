CREATE UNIQUE INDEX IF NOT EXISTS
    bearer_token_unique_for_scim_client
ON
    omicron.public.scim_client_bearer_token (bearer_token)
WHERE
    time_deleted IS NULL;
