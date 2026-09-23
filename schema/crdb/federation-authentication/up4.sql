ALTER TABLE omicron.public.audit_log ADD CONSTRAINT IF NOT EXISTS actor_kind_and_id_consistent CHECK (
    (actor_kind = 'user_builtin' AND actor_id IS NOT NULL AND actor_silo_id IS NULL)
    OR (actor_kind = 'silo_user' AND actor_id IS NOT NULL AND actor_silo_id IS NOT NULL)
    OR (actor_kind = 'scim' AND actor_id IS NULL AND actor_silo_id IS NOT NULL)
    OR (actor_kind = 'federated' AND actor_id IS NOT NULL AND actor_silo_id IS NOT NULL)
    OR (actor_kind = 'unauthenticated' AND actor_id IS NULL AND actor_silo_id IS NULL)
);
