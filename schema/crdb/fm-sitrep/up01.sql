CREATE TABLE IF NOT EXISTS omicron.public.fm_sitrep (
    -- The ID of this sitrep.
    id UUID PRIMARY KEY,
    --  The ID of the parent sitrep.
    --
    -- A sitrep's _parent_ is the sitrep that was current when the planning
    -- phase that produced that sitrep ran. The parent sitrep is a planning
    -- input that produced this sitrep.
    --
    -- This is effectively a foreign key back to this table; however, it is
    -- allowed to be NULL: the initial sitrep has no parent. Additionally,
    -- it may be non-NULL but no longer reference a row in this table: once a
    -- child sitrep has been created from a parent, it's possible for the
    -- parent to be deleted. We do not NULL out this field on such a deletion,
    -- so we can always see that there had been a particular parent even if
    -- it's now gone.
    parent_sitrep_id UUID,
    -- The ID of the inventory collection that was used as input to this
    -- sitrep.
    --
    -- This is a foreign key that references a row in the `inv_collection`
    -- table (and other inventory records associated with that collection).
    --
    -- Note that inventory collections are pruned on a separate schedule
    -- from sitreps, so the inventory collection records may not exist.
    inv_collection_id UUID NOT NULL,

    -- These fields are not semantically meaningful and are intended
    -- debugging purposes.

    -- The time at which this sitrep was created.
    time_created TIMESTAMPTZ NOT NULL,
    -- The Omicron zone UUID of the Nexus instance that created this
    -- sitrep.
    creator_id UUID NOT NULL,
    -- A human-readable description of the changes represented by this
    -- sitrep.
    comment TEXT NOT NULL
);
