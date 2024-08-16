CREATE INDEX IF NOT EXISTS ipv4_nat_lookup_by_vni ON omicron.public.ipv4_nat_entry (
  vni
)
STORING (
  external_address,
  first_port,
  last_port,
  sled_address,
  mac,
  version_added,
  version_removed,
  time_created,
  time_deleted
);
