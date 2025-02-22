CREATE INDEX IF NOT EXISTS lookup_affinity_group_instance_membership_by_instance ON omicron.public.affinity_group_instance_membership (
    instance_id
);

