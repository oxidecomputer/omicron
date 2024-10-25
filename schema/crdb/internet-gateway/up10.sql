CREATE INDEX IF NOT EXISTS inv_omicron_zone_nic_id ON omicron.public.inv_omicron_zone 
    (nic_id) STORING (sled_id, primary_service_ip, second_service_ip, snat_ip);
