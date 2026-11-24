CREATE TABLE IF NOT EXISTS omicron.public.wicket_ssh_auth_key (
    generation INT8 NOT NULL,
    authorized_key STRING NOT NULL,
    description STRING NOT NULL,

    PRIMARY KEY (generation, authorized_key)
);
