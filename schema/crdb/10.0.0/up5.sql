CREATE INDEX IF NOT EXISTS ipv4_nat_lookup ON omicron.public.ipv4_nat_entry (external_address, first_port, last_port, sled_address, vni, mac);
