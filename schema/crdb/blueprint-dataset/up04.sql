-- description of an omicron dataset specified in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_dataset (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,

    -- Dataset disposition
    disposition omicron.public.bp_dataset_disposition NOT NULL,

    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    -- Only valid if kind = zone
    zone_name TEXT,

    -- Only valid if kind = crucible
    ip INET,
    port INT4 CHECK (port BETWEEN 0 AND 65535),

    quota INT8,
    reservation INT8,
    compression TEXT,

    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone') OR
      (kind = 'zone' AND zone_name IS NOT NULL)
    ),

    CONSTRAINT ip_and_port_set_for_crucible CHECK (
      (kind != 'crucible') OR
      (kind = 'crucible' AND ip IS NOT NULL and port IS NOT NULL)
    ),

    PRIMARY KEY (blueprint_id, id)
)
