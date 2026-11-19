SET LOCAL disallow_full_table_scans = off;
-- Instances could previously be added to a group from a different project.
-- That is now rejected, so remove any such memberships.
DELETE FROM omicron.public.affinity_group_instance_membership
    WHERE (group_id, instance_id) IN (
        SELECT m.group_id, m.instance_id
        FROM omicron.public.affinity_group_instance_membership AS m
        JOIN omicron.public.affinity_group AS g ON g.id = m.group_id
        JOIN omicron.public.instance AS i ON i.id = m.instance_id
        WHERE g.project_id != i.project_id
    );
