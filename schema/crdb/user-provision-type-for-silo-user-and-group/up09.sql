UPDATE
 omicron.public.silo_group
SET
 user_provision_type = silo.user_provision_type
FROM
 silo
WHERE
 silo.id = silo_group.silo_id AND silo_group.time_deleted is null;
