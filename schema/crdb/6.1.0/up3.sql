CREATE UNIQUE INDEX IF NOT EXISTS ipv4_nat_gen ON omicron.public.ipv4_nat_entry (
    gen
)
STORING (
    external_address,
    first_port,
    last_port,
    sled_address,
    vni,
    mac,
    time_created,
    time_deleted
);
