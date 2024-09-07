CREATE TYPE IF NOT EXISTS omicron.public.root_of_trust_page_which AS ENUM (
    'cmpa',
    'cfpa_active',
    'cfpa_inactive',
    'cfpa_scratch'
);
