CREATE INDEX IF NOT EXISTS lookup_anti_affinity_group_affinity_membership_by_affinity_group ON omicron.public.anti_affinity_group_affinity_membership (
    affinity_group_id
);

