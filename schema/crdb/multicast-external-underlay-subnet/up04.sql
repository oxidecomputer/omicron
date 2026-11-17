-- Restrict underlay NAT targets to Oxide's ff04::/64 allocation, as validated
-- by Dendrite's UnderlayMulticastIpv6. RFC 7346 §2 defines the enclosing
-- scope.
ALTER TABLE omicron.public.underlay_multicast_group
    ADD CONSTRAINT IF NOT EXISTS underlay_ipv6_admin_scoped CHECK (
        family(multicast_ip) = 6 AND multicast_ip << 'ff04::/64'
    );
