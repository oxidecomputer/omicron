UPDATE
 omicron.public.silo_user
SET
 user_provision_type = silo.user_provision_type
FROM
 silo
WHERE
 silo.id = silo_user.silo_id AND silo_user.time_deleted is null;
