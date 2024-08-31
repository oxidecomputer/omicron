CREATE TYPE IF NOT EXISTS omicron.public.instance_auto_restart AS ENUM (
    /*
     * The instance should not, under any circumstances, be automatically
     * rebooted by the control plane.
     */
    'never',
    /*
     * The instance should be automatically restarted any time a fault is
     * detected
     */
    'all_failures'
);
