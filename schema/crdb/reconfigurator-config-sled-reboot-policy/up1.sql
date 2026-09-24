CREATE TYPE IF NOT EXISTS
omicron.public.reconfigurator_planner_sled_reboot_policy AS ENUM (
    'immediate_no_evacuation',
    'evacuate'
);
