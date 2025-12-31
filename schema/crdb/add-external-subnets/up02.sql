CREATE TABLE IF NOT EXISTS omicron.public.external_subnet (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    external_subnet_pool_id UUID NOT NULL,
    subnet INET NOT NULL,
    min_alloc_prefix INT2 NOT NULL,
    max_alloc_prefix INT2 NOT NULL,
    rcgen INT8 NOT NULL,
    CONSTRAINT valid_prefix_sizes (
        min_alloc_prefix >= 0 AND
        max_alloc_prefix >= 0 AND
        (
            (family(subnet) = 4 AND min_alloc_prefix <= 32) OR
            (family(subnet) = 6 AND min_alloc_prefix <= 128)
        ) AND
        (
            (family(subnet) = 4 AND max_alloc_prefix <= 32) OR
            (family(subnet) = 6 AND max_alloc_prefix <= 128)
        ),
    CONSTRAINT min_prefix_no_larger_than_max_prefix (
        min_alloc_prefix <= max_alloc_prefix
    )
);
