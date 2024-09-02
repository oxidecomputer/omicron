CREATE TYPE IF NOT EXISTS omicron.public.instance_auto_restart AS ENUM (
    /*
     * The instance should not, under any circumstances, be automatically
     * rebooted by the control plane.
     */
    'never',
    /*
     * The instance should be automatically restarted if, and only if, the sled
     * it was running on has restarted or become unavailable. If the individual
     * Propolis VMM process for this instance crashes, it should *not* be
     * restarted automatically.
     */
     'sled_failures_only',
    /*
     * The instance should be automatically restarted any time a fault is
     * detected
     */
    'all_failures'
);
