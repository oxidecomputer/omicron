-- Simplify IPv6 multicast address validation
-- Only exclude ff04::/16 (admin-local, reserved for underlay groups)
-- Remove unnecessary exclusions for ff05::/16 and ff08::/16

ALTER TABLE omicron.public.multicast_group
    DROP CONSTRAINT IF EXISTS external_multicast_ip_valid;

ALTER TABLE omicron.public.multicast_group
    ADD CONSTRAINT external_multicast_ip_valid CHECK (
        (family(multicast_ip) = 4 AND multicast_ip << '224.0.0.0/4') OR
        (family(multicast_ip) = 6 AND multicast_ip << 'ff00::/8' AND
         NOT multicast_ip << 'ff04::/16')
    );
