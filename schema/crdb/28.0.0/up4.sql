CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zone_nic (
    blueprint_id UUID NOT NULL,
    id UUID NOT NULL,
    name TEXT NOT NULL,
    ip INET NOT NULL,
    mac INT8 NOT NULL,
    subnet INET NOT NULL,
    vni INT8 NOT NULL,
    is_primary BOOLEAN NOT NULL,
    slot INT2 NOT NULL,

    PRIMARY KEY (blueprint_id, id)
);
