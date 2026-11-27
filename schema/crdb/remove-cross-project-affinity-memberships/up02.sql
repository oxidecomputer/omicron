SET LOCAL disallow_full_table_scans = off;
-- As in up01.sql, for anti-affinity groups.
DELETE FROM omicron.public.anti_affinity_group_instance_membership
    WHERE (group_id, instance_id) IN (
        SELECT m.group_id, m.instance_id
        FROM omicron.public.anti_affinity_group_instance_membership AS m
        JOIN omicron.public.anti_affinity_group AS g ON g.id = m.group_id
        JOIN omicron.public.instance AS i ON i.id = m.instance_id
        WHERE g.project_id != i.project_id
    );
