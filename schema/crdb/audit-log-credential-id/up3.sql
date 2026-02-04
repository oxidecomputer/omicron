-- NOT VALID skips validation of existing rows. Existing audit_log entries
-- may have auth_method set but credential_id NULL (since credential_id didn't
-- exist before this migration). The constraint will be enforced for new rows.
ALTER TABLE omicron.public.audit_log
ADD CONSTRAINT IF NOT EXISTS auth_method_and_credential_id_consistent CHECK (
    (auth_method IS NULL AND credential_id IS NULL)
    OR (auth_method = 'spoof' AND credential_id IS NULL)
    OR (auth_method IN ('session_cookie', 'access_token', 'scim_token')
        AND credential_id IS NOT NULL)
) NOT VALID;
