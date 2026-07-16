/* The unique identity for a membership row is
   `(external_group_id, parent_kind, parent_id)` now that parents may be
   either an instance or a probe. Add the kind-aware unique index first so
   uniqueness is enforced throughout the swap. */
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_unique_kind_parent_per_group
    ON omicron.public.multicast_group_member (
        external_group_id,
        parent_kind,
        parent_id
    ) WHERE time_deleted IS NULL;
