<<<<<<< HEAD
CREATE TABLE IF NOT EXISTS omicron.public.instance_ssh_key (
    instance_id UUID NOT NULL,
    ssh_key_id UUID NOT NULL,
    PRIMARY KEY (instance_id, ssh_key_id)
);
=======
UPDATE omicron.public.sled
    SET last_used_address = (netmask(set_masklen(ip, 64)) & ip) + 0xFFFF
    WHERE time_deleted is null;
>>>>>>> main
