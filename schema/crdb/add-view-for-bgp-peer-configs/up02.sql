CREATE INDEX IF NOT EXISTS switch_port_id_and_name
ON omicron.public.switch_port (port_settings_id, port_name) STORING (switch_location);
