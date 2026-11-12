CREATE TYPE IF NOT EXISTS omicron.public.sled_expunge_sp_slot_source AS ENUM (
    'inventory',
    'live_scan',
    'found_after_absent',
    'relocated'
);
