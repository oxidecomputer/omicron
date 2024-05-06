CREATE TABLE IF NOT EXISTS omicron.public.allow_list (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    allowed_ips INET[] CHECK (array_length(allowed_ips, 1) > 0)
);
