CREATE TABLE IF NOT EXISTS omicron.public.ipv4_nat_entry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_address INET CONSTRAINT ext_addr_not_null NOT NULL,
    first_port INT4 CONSTRAINT first_port_not_null NOT NULL,
    last_port INT4 CONSTRAINT last_port_not_null NOT NULL,
    sled_address INET CONSTRAINT sled_addr_not_null NOT NULL,
    vni INT4 CONSTRAINT vni_not_null NOT NULL,
    mac INT8 CONSTRAINT mac_not_null NOT NULL,
    gen INT8 CONSTRAINT gen_not_null NOT NULL DEFAULT nextval('omicron.public.nat_gen') ON UPDATE nextval('omicron.public.nat_gen'),
    time_created TIMESTAMPTZ CONSTRAINT tc_addr_not_null NOT NULL DEFAULT now(),
    time_deleted TIMESTAMPTZ
);
