CREATE TABLE IF NOT EXISTS physical_disk_adoption_request (
    id UUID PRIMARY KEY,
	vendor STRING(63) NOT NULL,
    model STRING(63) NOT NULL,
    serial STRING(63) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);
