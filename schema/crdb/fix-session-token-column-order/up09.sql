-- Create device_access_token_new with correct column order (id first)
CREATE TABLE IF NOT EXISTS omicron.public.device_access_token_new (
    id UUID PRIMARY KEY,
    token STRING(40) NOT NULL,
    client_id UUID NOT NULL,
    device_code STRING(40) NOT NULL,
    silo_user_id UUID NOT NULL,
    time_requested TIMESTAMPTZ NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ
);
