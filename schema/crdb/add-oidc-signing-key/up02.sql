CREATE UNIQUE INDEX IF NOT EXISTS lookup_oidc_signing_key_by_kid
    ON omicron.public.oidc_signing_key (kid) WHERE time_deleted IS NULL;
