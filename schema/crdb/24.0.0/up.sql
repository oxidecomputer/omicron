UPDATE omicron.public.sled
    SET last_used_address = (netmask(set_masklen(ip, 64)) & ip) + 0xFFFF
    WHERE time_deleted is null;
