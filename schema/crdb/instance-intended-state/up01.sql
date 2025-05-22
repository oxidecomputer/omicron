/*
 * Represents the *desired* state of an instance, as requested by the user.
*/
CREATE TYPE IF NOT EXISTS omicron.public.instance_intended_state AS ENUM (
    /* The instance should be running. */
    'running',

    /* The instance was asked to stop by an API request. */
    'stopped',

    /* The guest OS shut down the virtual machine.
     *
     * This is distinct from the 'stopped' intent, which represents a stop
     * requested by the API.
     */
    'guest_shutdown',

    /* The instance should be destroyed. */
    'destroyed'
);
