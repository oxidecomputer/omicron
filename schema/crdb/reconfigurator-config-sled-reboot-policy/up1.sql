CREATE TYPE IF NOT EXISTS
omicron.public.sled_update_reboot_policy AS ENUM (
    'immediate_no_evacuation',
    'evacuate'
);
