/* Now add the CHECK constraint ensuring we have a matching IP */
ALTER TABLE
omicron.public.network_interface
ADD CONSTRAINT transit_ips_require_ip_address
CHECK (
    (array_length(transit_ips, 1) = 0 OR ip IS NOT NULL) AND
    (array_length(transit_ips_v6, 1) = 0 OR ipv6 IS NOT NULL)
);
