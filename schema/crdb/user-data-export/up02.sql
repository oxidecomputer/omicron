CREATE TABLE IF NOT EXISTS omicron.public.user_data_export (
    id UUID PRIMARY KEY,
    resource_id UUID NOT NULL,
    resource_type omicron.public.user_data_export_resource_type NOT NULL,
    pantry_ip INET NOT NULL,
    pantry_port INT4 CHECK (pantry_port BETWEEN 0 AND 65535) NOT NULL,
    volume_id UUID NOT NULL,
    resource_deleted BOOL NOT NULL
);
