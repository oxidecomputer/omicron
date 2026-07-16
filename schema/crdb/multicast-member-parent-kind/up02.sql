/* Add `parent_kind` to `multicast_group_member` so the reconciler can
   dispatch on parent kind without joining the `instance` and `probe`
   tables on every row. Probe membership is introduced by this change,
   so existing rows are all instance-typed and the backfill default is
   'instance'. */
ALTER TABLE omicron.public.multicast_group_member
    ADD COLUMN IF NOT EXISTS parent_kind
        omicron.public.multicast_group_member_parent_kind
        NOT NULL DEFAULT 'instance';
