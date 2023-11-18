-- Add an index which allows pagination by {rack_id, sled_id} pairs. 
CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_allocation_by_rack_and_sled ON omicron.public.sled_underlay_subnet_allocation (
    rack_id,
    sled_id
);
