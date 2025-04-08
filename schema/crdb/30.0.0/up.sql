
CREATE TABLE IF NOT EXISTS omicron.public.instance_ssh_key (
    instance_id UUID NOT NULL,
    ssh_key_id UUID NOT NULL,
    PRIMARY KEY (instance_id, ssh_key_id)
);