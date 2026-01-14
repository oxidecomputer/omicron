ALTER TABLE omicron.public.audit_log
ADD CONSTRAINT IF NOT EXISTS auth_method_and_credential_id_consistent CHECK (
    (auth_method IS NULL AND credential_id IS NULL)
    OR (auth_method = 'spoof' AND credential_id IS NULL)
    OR (auth_method IN ('session_cookie', 'access_token', 'scim_token')
        AND credential_id IS NOT NULL)
);
