ALTER TABLE omicron.public.sled_underlay_subnet_allocation
    ALTER PRIMARY KEY USING COLUMNS (hw_baseboard_id, sled_id);
