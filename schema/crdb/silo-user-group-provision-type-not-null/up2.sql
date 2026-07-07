-- The CHECK constraint is now redundant: NOT NULL on user_provision_type
-- subsumes the condition that was previously enforced only for non-deleted rows.

ALTER TABLE omicron.public.silo_user
  DROP CONSTRAINT IF EXISTS user_provision_type_required_for_non_deleted;
