-- External multicast groups (customer-facing, allocated from IP pools)
CREATE TABLE IF NOT EXISTS omicron.public.multicast_group (
    /* Identity metadata (following Resource pattern) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* VNI for multicast group (derived or random) */
    vni INT4 NOT NULL,

    /* IP allocation from pools */
    ip_pool_id UUID NOT NULL,
    ip_pool_range_id UUID NOT NULL,

    /* IP assigned to this multicast group */
    multicast_ip INET NOT NULL,

    /* Source-Specific Multicast (SSM) support */
    source_ips INET[] DEFAULT ARRAY[]::INET[],

    /* Multicast VLAN (MVLAN) for egress to upstream networks */
    /* Tags packets leaving the rack to traverse VLAN-segmented upstream networks */
    /* Internal rack traffic uses VNI-based underlay forwarding */
    mvlan INT2,

    /* Associated underlay group for NAT */
    /* We fill this as part of the RPW */
    underlay_group_id UUID,

    /* DPD tag to couple external/underlay state for this group */
    tag STRING(63),

    /* Current state of the multicast group (for RPW) */
    state omicron.public.multicast_group_state NOT NULL DEFAULT 'creating',

    /* Sync versioning */
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.multicast_group_version'),
    version_removed INT8,

    /* Constraints */
    -- External groups: IPv4 multicast or non-admin-scoped IPv6
    CONSTRAINT external_multicast_ip_valid CHECK (
        (family(multicast_ip) = 4 AND multicast_ip << '224.0.0.0/4') OR
        (family(multicast_ip) = 6 AND multicast_ip << 'ff00::/8' AND
         NOT multicast_ip << 'ff04::/16' AND
         NOT multicast_ip << 'ff05::/16' AND
         NOT multicast_ip << 'ff08::/16')
    ),

    -- Reserved range validation for IPv4
    CONSTRAINT external_ipv4_not_reserved CHECK (
        family(multicast_ip) != 4 OR (
            family(multicast_ip) = 4 AND
            NOT multicast_ip << '224.0.0.0/24' AND     -- Link-local control block
            NOT multicast_ip << '233.0.0.0/8' AND      -- GLOP addressing
            NOT multicast_ip << '239.0.0.0/8'          -- Administratively scoped
        )
    ),

    -- Reserved range validation for IPv6
    CONSTRAINT external_ipv6_not_reserved CHECK (
        family(multicast_ip) != 6 OR (
            family(multicast_ip) = 6 AND
            NOT multicast_ip << 'ff01::/16' AND         -- Interface-local scope
            NOT multicast_ip << 'ff02::/16'             -- Link-local scope
        )
    ),

    -- MVLAN validation (Dendrite requires >= 2)
    CONSTRAINT mvlan_valid_range CHECK (
        mvlan IS NULL OR (mvlan >= 2 AND mvlan <= 4094)
    )
);
