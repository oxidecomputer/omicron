CREATE UNIQUE INDEX IF NOT EXISTS overlapping_ipv4_nat_entry ON omicron.public.ipv4_nat_entry (
    external_address,
    first_port,
    last_port
) WHERE time_deleted IS NULL;
