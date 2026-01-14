-- Salt for underlay IP collision avoidance (XORed into mapping)
ALTER TABLE omicron.public.multicast_group
    ADD COLUMN IF NOT EXISTS underlay_salt INT2;
