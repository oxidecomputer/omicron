CREATE UNIQUE INDEX IF NOT EXISTS lookup_baseboard_id_by_props
    ON omicron.public.hw_baseboard_id (part_number, serial_number);
