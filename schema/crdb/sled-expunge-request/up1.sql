CREATE TYPE IF NOT EXISTS omicron.public.sled_expunge_request_state AS ENUM (
    'waiting_for_trust_quorum',
    'powering_off',
    'expunging',
    'done',
    'failed'
);
