CREATE TABLE IF NOT EXISTS omicron.public.user_data_export (
    id UUID PRIMARY KEY,

    state omicron.public.user_data_export_state NOT NULL,
    operating_saga_id UUID,
    generation INT8 NOT NULL,

    resource_id UUID NOT NULL,
    resource_type omicron.public.user_data_export_resource_type NOT NULL,
    resource_deleted BOOL NOT NULL,

    pantry_ip INET,
    pantry_port INT4 CHECK (pantry_port BETWEEN 0 AND 65535),
    volume_id UUID
);
