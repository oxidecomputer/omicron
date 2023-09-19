-- to get ready to drop the internal column, take any IP pools with internal =
-- true and set silo_id = INTERNAL_SILO_ID

UPDATE omicron.public.ip_pool
    SET silo_id = '001de000-5110-4000-8000-000000000001'
    WHERE internal = true and time_deleted is null;

UPDATE omicron.public.ip_pool
    SET is_default = true
    WHERE name = 'default' and time_deleted is null;
