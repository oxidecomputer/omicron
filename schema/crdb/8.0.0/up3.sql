CREATE TYPE IF NOT EXISTS omicron.public.switch_link_fec AS ENUM (
    'Firecode',
    'None',
    'Rs'
);

CREATE TYPE IF NOT EXISTS omicron.public.switch_link_speed AS ENUM (
    '0G',
    '1G',
    '10G',
    '25G',
    '40G',
    '50G',
    '100G',
    '200G',
    '400G'
);

ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS fec omicron.public.switch_link_fec;
ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS speed omicron.public.switch_link_speed;

ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS hold_time INT8;
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS idle_hold_time INT8;
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS delay_open INT8;
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS connect_retry INT8;
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS keepalive INT8;
