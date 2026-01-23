CREATE TABLE IF NOT EXISTS omicron.public.subnet_pool_member (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    subnet_pool_id UUID NOT NULL,
    subnet INET NOT NULL,
    first_address INET AS (subnet & netmask(subnet)) VIRTUAL,
    last_address INET AS (
        broadcast(subnet) & (netmask(subnet) | hostmask(subnet))
    ) VIRTUAL,
    min_prefix_length INT2 NOT NULL,
    max_prefix_length INT2 NOT NULL,
    rcgen INT8 NOT NULL,
    CONSTRAINT valid_prefix_sizes CHECK (
        min_prefix_length >= 0 AND
        max_prefix_length >= 0 AND
        (
            (family(subnet) = 4 AND min_prefix_length <= 32) OR
            (family(subnet) = 6 AND min_prefix_length <= 128)
        ) AND
        (
            (family(subnet) = 4 AND max_prefix_length <= 32) OR
            (family(subnet) = 6 AND max_prefix_length <= 128)
        )
    ),
    CONSTRAINT min_prefix_no_larger_than_max_prefix CHECK (
        min_prefix_length <= max_prefix_length
    )
);
