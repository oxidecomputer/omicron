CREATE UNIQUE INDEX IF NOT EXISTS ipv4_nat_version_removed ON omicron.public.ipv4_nat_entry (
    version_removed
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
