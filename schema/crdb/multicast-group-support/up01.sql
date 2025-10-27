-- Multicast group support: Add multicast groups and membership (RFD 488)

-- Create versioning sequence for multicast group changes
CREATE SEQUENCE IF NOT EXISTS omicron.public.multicast_group_version START 1 INCREMENT 1;

-- Multicast group state for RPW
CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_state AS ENUM (
    'creating',
    'active',
    'deleting',
    'deleted'
);

-- Multicast group member state for RPW pattern
CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_member_state AS ENUM (
    'joining',
    'joined',
    'left'
);

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

    /* Rack ID where the group was created */
    rack_id UUID NOT NULL,

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

-- Multicast group membership (external groups)
CREATE TABLE IF NOT EXISTS omicron.public.multicast_group_member (
    /* Identity */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* External group for customer/external membership */
    external_group_id UUID NOT NULL,

    /* Parent instance or service */
    parent_id UUID NOT NULL,

    /* Sled hosting the parent instance (denormalized for performance) */
    /* NULL when instance is stopped, populated when active */
    sled_id UUID,

    /* RPW state for reliable operations */
    state omicron.public.multicast_group_member_state NOT NULL,

    /* Sync versioning */
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.multicast_group_version'),
    version_removed INT8
);

/* External Multicast Group Indexes */

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS multicast_group_version_added ON omicron.public.multicast_group (
    version_added
) STORING (
    name,
    multicast_ip,
    time_created,
    time_deleted
);

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS multicast_group_version_removed ON omicron.public.multicast_group (
    version_removed
) STORING (
    name,
    multicast_ip,
    time_created,
    time_deleted
);

-- IP address uniqueness and conflict detection
-- Supports: SELECT ... WHERE multicast_ip = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_multicast_by_ip ON omicron.public.multicast_group (
    multicast_ip
) WHERE time_deleted IS NULL;

-- Pool management and allocation queries
-- Supports: SELECT ... WHERE ip_pool_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS external_multicast_by_pool ON omicron.public.multicast_group (
    ip_pool_id,
    ip_pool_range_id
) WHERE time_deleted IS NULL;

-- Underlay NAT group association
-- Supports: SELECT ... WHERE underlay_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS external_multicast_by_underlay ON omicron.public.multicast_group (
    underlay_group_id
) WHERE time_deleted IS NULL AND underlay_group_id IS NOT NULL;

-- State-based filtering for RPW reconciler
-- Supports: SELECT ... WHERE state = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_group_by_state ON omicron.public.multicast_group (
    state
) WHERE time_deleted IS NULL;

-- RPW reconciler composite queries (state + pool filtering)
-- Supports: SELECT ... WHERE state = ? AND ip_pool_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_group_reconciler_query ON omicron.public.multicast_group (
    state,
    ip_pool_id
) WHERE time_deleted IS NULL;

-- Fleet-wide unique name constraint (groups are fleet-scoped like IP pools)
-- Supports: SELECT ... WHERE name = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_multicast_group_by_name ON omicron.public.multicast_group (
    name
) WHERE time_deleted IS NULL;

/* Underlay Multicast Group Indexes */

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS underlay_multicast_group_version_added ON omicron.public.underlay_multicast_group (
    version_added
) STORING (
    multicast_ip,
    time_created,
    time_deleted
);

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS underlay_multicast_group_version_removed ON omicron.public.underlay_multicast_group (
    version_removed
) STORING (
    multicast_ip,
    time_created,
    time_deleted
);

-- Admin-scoped IPv6 address uniqueness
-- Supports: SELECT ... WHERE multicast_ip = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS lookup_underlay_multicast_by_ip ON omicron.public.underlay_multicast_group (
    multicast_ip
) WHERE time_deleted IS NULL;

-- Lifecycle management via group tags
-- Supports: SELECT ... WHERE tag = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS underlay_multicast_by_tag ON omicron.public.underlay_multicast_group (
    tag
) WHERE time_deleted IS NULL AND tag IS NOT NULL;

/* Multicast Group Member Indexes */

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_added >= ? ORDER BY version_added
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_version_added ON omicron.public.multicast_group_member (
    version_added
) STORING (
    external_group_id,
    parent_id,
    time_created,
    time_deleted
);

-- Version tracking for Omicron internal change detection
-- Supports: SELECT ... WHERE version_removed >= ? ORDER BY version_removed
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_version_removed ON omicron.public.multicast_group_member (
    version_removed
) STORING (
    external_group_id,
    parent_id,
    time_created,
    time_deleted
);

-- Group membership listing and pagination
-- Supports: SELECT ... WHERE external_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_external_group ON omicron.public.multicast_group_member (
    external_group_id
) WHERE time_deleted IS NULL;

-- Instance membership queries (all groups for an instance)
-- Supports: SELECT ... WHERE parent_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_parent ON omicron.public.multicast_group_member (
    parent_id
) WHERE time_deleted IS NULL;

-- RPW reconciler sled-based switch port resolution
-- Supports: SELECT ... WHERE sled_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_sled ON omicron.public.multicast_group_member (
    sled_id
) WHERE time_deleted IS NULL;

-- Instance-focused composite queries with group filtering
-- Supports: SELECT ... WHERE parent_id = ? AND external_group_id = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_by_parent_and_group ON omicron.public.multicast_group_member (
    parent_id,
    external_group_id
) WHERE time_deleted IS NULL;

-- Business logic constraint: one instance per group (also serves queries)
-- Supports: SELECT ... WHERE external_group_id = ? AND parent_id = ? AND time_deleted IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS multicast_member_unique_parent_per_group ON omicron.public.multicast_group_member (
    external_group_id,
    parent_id
) WHERE time_deleted IS NULL;

-- RPW reconciler state processing by group
-- Supports: SELECT ... WHERE external_group_id = ? AND state = ? AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_group_state ON omicron.public.multicast_group_member (
    external_group_id,
    state
) WHERE time_deleted IS NULL;

-- RPW cleanup of soft-deleted members
-- Supports: DELETE FROM multicast_group_member WHERE state = 'Left' AND time_deleted IS NOT NULL
CREATE INDEX IF NOT EXISTS multicast_member_cleanup ON omicron.public.multicast_group_member (
    state
) WHERE time_deleted IS NOT NULL;

-- Saga unwinding hard deletion by group
-- Supports: DELETE FROM multicast_group_member WHERE external_group_id = ?
CREATE INDEX IF NOT EXISTS multicast_member_hard_delete_by_group ON omicron.public.multicast_group_member (
    external_group_id
);

-- Pagination optimization for group member listing
-- Supports: SELECT ... WHERE external_group_id = ? ORDER BY id LIMIT ? OFFSET ?
CREATE INDEX IF NOT EXISTS multicast_member_group_id_order ON omicron.public.multicast_group_member (
    external_group_id,
    id
) WHERE time_deleted IS NULL;

-- Pagination optimization for instance member listing
-- Supports: SELECT ... WHERE parent_id = ? ORDER BY id LIMIT ? OFFSET ?
CREATE INDEX IF NOT EXISTS multicast_member_parent_id_order ON omicron.public.multicast_group_member (
    parent_id,
    id
) WHERE time_deleted IS NULL;

-- Instance lifecycle state transitions optimization
-- Supports: UPDATE ... WHERE parent_id = ? AND state IN (?, ?) AND time_deleted IS NULL
CREATE INDEX IF NOT EXISTS multicast_member_parent_state ON omicron.public.multicast_group_member (
    parent_id,
    state
) WHERE time_deleted IS NULL;
