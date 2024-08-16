ALTER TABLE omicron.public.instance
ADD CONSTRAINT IF NOT EXISTS vmm_iff_active_propolis CHECK (
    ((state = 'vmm') AND (active_propolis_id IS NOT NULL)) OR
    ((state != 'vmm') AND (active_propolis_id IS NULL))
)
