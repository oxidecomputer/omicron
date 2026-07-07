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
