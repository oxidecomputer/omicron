CREATE UNIQUE INDEX IF NOT EXISTS federation_role_grant_policy_resource_role
ON omicron.public.federation_role_grant
(trust_policy_id, resource_kind, resource_id, role_name);
