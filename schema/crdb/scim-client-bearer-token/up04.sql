ALTER TYPE
 omicron.public.audit_log_actor_kind
ADD VALUE IF NOT EXISTS
 'scim'
AFTER
 'unauthenticated';
