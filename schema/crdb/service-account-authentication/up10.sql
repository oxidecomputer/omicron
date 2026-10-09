ALTER TABLE omicron.public.audit_log ADD CONSTRAINT IF NOT EXISTS federation_identity_consistent CHECK (
    (federation_idp_id IS NULL AND federation_iss IS NULL AND federation_sub IS NULL)
    OR (federation_idp_id IS NOT NULL AND federation_iss IS NOT NULL AND federation_sub IS NOT NULL
        AND actor_kind = 'service_account' AND auth_method IS NOT NULL
        AND auth_method = 'service_account_token' AND credential_id IS NOT NULL)
);
