CREATE INDEX IF NOT EXISTS instance_network_interface_mac
    ON omicron.public.network_interface (mac) STORING (time_deleted);
