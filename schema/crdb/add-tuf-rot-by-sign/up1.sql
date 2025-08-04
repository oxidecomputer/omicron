CREATE TABLE IF NOT EXISTS omicron.public.tuf_rot_by_sign (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    version STRING(63) NOT NULL,
    kind STRING(63) NOT NULL,
    sign Bytes NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    CONSTRAINT unique_name_version_kind_sign UNIQUE (name, version, kind, sign)
);
