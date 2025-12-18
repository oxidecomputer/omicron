CREATE TABLE IF NOT EXISTS omicron.public.disk_type_crucible (
    disk_id UUID PRIMARY KEY,
    volume_id UUID NOT NULL,
    origin_snapshot UUID,
    origin_image UUID,
    pantry_address TEXT
);
