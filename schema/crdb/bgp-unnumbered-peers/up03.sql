-- Drop the NOT NULL constraints on columns that were part of the old primary key.
-- These constraints were implicitly added when the columns were part of the pk,
-- and they persist after dropping the pk. We need to explicitly drop them since
-- these columns are now nullable (port_settings_id and interface_name) or
-- intentionally nullable for unnumbered peers (addr).
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
    ALTER COLUMN id DROP DEFAULT,
    ALTER COLUMN port_settings_id DROP NOT NULL,
    ALTER COLUMN interface_name DROP NOT NULL,
    ALTER COLUMN addr DROP NOT NULL;
