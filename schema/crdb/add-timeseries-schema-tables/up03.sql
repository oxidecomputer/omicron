CREATE TYPE IF NOT EXISTS omicron.public.timeseries_field_type AS ENUM (
    'string',
    'i8',
    'u8',
    'i16',
    'u16',
    'i32',
    'u32',
    'i64',
    'u64',
    'ip_addr',
    'uuid',
    'bool'
);
