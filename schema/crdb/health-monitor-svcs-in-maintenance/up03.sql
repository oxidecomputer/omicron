CREATE TABLE IF NOT EXISTS omicron.public.inv_health_monitor_svc_in_maintenance_error (
    -- where this observation came from
    -- (foreign key into `inv_health_monitor_svc_in_maintenance` table)
    svcs_in_maintenance_id UUID NOT NULL,

    -- unique id for each row
    id UUID NOT NULL,

    -- an error message found when retrieving the SMF services in maintenance
    error_message TEXT,

    PRIMARY KEY (svcs_in_maintenance_id, id)
);