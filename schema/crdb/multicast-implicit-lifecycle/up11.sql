-- Add ff00::/16 (reserved scope) to IPv6 blocked ranges for consistency with API

ALTER TABLE omicron.public.multicast_group
    DROP CONSTRAINT IF EXISTS external_ipv6_not_reserved;

ALTER TABLE omicron.public.multicast_group
    ADD CONSTRAINT external_ipv6_not_reserved CHECK (
        family(multicast_ip) != 6 OR (
            family(multicast_ip) = 6 AND
            NOT multicast_ip << 'ff00::/16' AND
            NOT multicast_ip << 'ff01::/16' AND
            NOT multicast_ip << 'ff02::/16'
        )
    );
