-- Exclude Oxide's ff04::/64 underlay allocation from external groups.
-- External pools may use the rest of the admin-local range (RFC 7346 §2).
ALTER TABLE omicron.public.multicast_group
    ADD CONSTRAINT IF NOT EXISTS external_multicast_ip_valid CHECK (
        (family(multicast_ip) = 4 AND multicast_ip << '224.0.0.0/4') OR
        (family(multicast_ip) = 6 AND multicast_ip << 'ff00::/8' AND
         NOT multicast_ip << 'ff04::/64')
    );
