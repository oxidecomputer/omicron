/*
 * Represents the *desired* state of an instance, as requested by the user.
*/
CREATE TYPE IF NOT EXISTS omicron.public.instance_intended_state AS ENUM (
    /* The instance should be running. */
    'running',

    /* The instance should be stopped */
    'stopped',

    /* The instance should be destroyed. */
    'destroyed'
);
