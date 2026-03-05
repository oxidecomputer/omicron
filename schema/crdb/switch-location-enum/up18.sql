ALTER TABLE omicron.public.switch_port ADD CONSTRAINT IF NOT EXISTS switch_port_rack_locaction_name_unique UNIQUE (
    rack_id, switch_loc, port_name
);
