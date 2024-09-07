CREATE TABLE IF NOT EXISTS omicron.public.hw_baseboard_id (
    id UUID PRIMARY KEY,
    part_number TEXT NOT NULL,
    serial_number TEXT NOT NULL
);
