CREATE UNIQUE INDEX IF NOT EXISTS commissioned_sled_uniqueness
    ON omicron.public.sled (serial_number, part_number)
    WHERE sled_state != 'decommissioned';
