DELETE FROM omicron.public.router_route WHERE 
time_deleted IS NULL AND
name = 'default-v4' AND 
vpc_router_id = '001de000-074c-4000-8000-000000000001';
