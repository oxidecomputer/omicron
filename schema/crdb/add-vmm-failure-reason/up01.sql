CREATE TYPE IF NOT EXISTS omicron.public.vmm_failure_reason AS ENUM (
    /* The VMM's sled was expunged. */
    'sled_expunged'
);
