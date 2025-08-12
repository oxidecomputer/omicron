ALTER TABLE IF EXISTS omicron.public.lldp_link_config
ADD COLUMN IF NOT EXISTS management_ip INET;
