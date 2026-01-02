ALTER TABLE
 omicron.public.audit_log
ADD CONSTRAINT IF NOT EXISTS
    actor_kind_and_id_consistent CHECK (
        -- For user_builtin: must have actor_id, must not have actor_silo_id
        (actor_kind = 'user_builtin' AND actor_id IS NOT NULL AND actor_silo_id IS NULL)
        OR
        -- For silo_user: must have both actor_id and actor_silo_id
        (actor_kind = 'silo_user' AND actor_id IS NOT NULL AND actor_silo_id IS NOT NULL)
        OR
        -- For a scim actor: must have a actor_silo_id
        (actor_kind = 'scim' AND actor_id IS NULL AND actor_silo_id IS NOT NULL)
        OR
        -- For unauthenticated: must not have actor_id or actor_silo_id
        (actor_kind = 'unauthenticated' AND actor_id IS NULL AND actor_silo_id IS NULL)
    );
