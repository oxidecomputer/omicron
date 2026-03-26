-- Simplify IPv4 multicast address validation
-- Only block link-local (224.0.0.0/24); allow GLOP and admin-scoped

ALTER TABLE omicron.public.multicast_group
    DROP CONSTRAINT IF EXISTS external_ipv4_not_reserved;

ALTER TABLE omicron.public.multicast_group
    ADD CONSTRAINT external_ipv4_not_reserved CHECK (
        family(multicast_ip) != 4 OR NOT multicast_ip << '224.0.0.0/24'
    );
