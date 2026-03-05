CREATE UNIQUE INDEX IF NOT EXISTS lookup_loopback_address ON omicron.public.loopback_address (
    address, rack_id, switch_loc
);
