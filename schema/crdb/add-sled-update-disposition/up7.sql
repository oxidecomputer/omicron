-- Existing rows satisfy this constraint, since they are backfilled to
-- 'available' with a NULL policy.
ALTER TABLE omicron.public.bp_sled_metadata
    ADD CONSTRAINT IF NOT EXISTS update_disruption_policy_set_iff_evacuating
    CHECK (
        (update_availability = 'evacuating')
            = (update_disruption_policy IS NOT NULL)
    );
