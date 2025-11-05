-- Drop the view which will depend on the `ipv6` column we're about to add.
DROP INDEX IF EXISTS omicron.public.network_interface_subnet_id_ip_key;
