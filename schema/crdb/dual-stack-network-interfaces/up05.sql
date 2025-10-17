-- Drop the index which depends on a column we're about to rename
DROP INDEX IF EXISTS omicron.public.network_interface_subnet_id_ip_key;
