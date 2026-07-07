-- Underlay multicast groups (admin-scoped IPv6 for VPC internal forwarding)
CREATE TABLE IF NOT EXISTS omicron.public.underlay_multicast_group (
    /* Identity */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* Admin-scoped IPv6 multicast address (NAT target) */
    multicast_ip INET NOT NULL,

    /* DPD tag to couple external/underlay state for this group */
    tag STRING(63),

    /* Sync versioning */
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.multicast_group_version'),
    version_removed INT8,

    /* Constraints */
    -- Underlay groups: admin-local scoped IPv6 only (ff04::/16)
    CONSTRAINT underlay_ipv6_admin_scoped CHECK (
        family(multicast_ip) = 6 AND multicast_ip << 'ff04::/16'
    )
);
