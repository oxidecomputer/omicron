CREATE TABLE IF NOT EXISTS omicron.public.inv_health_monitor_svc_in_maintenance_service (
    -- where this observation came from
    -- (foreign key into `inv_health_monitor_svc_in_maintenance` table)
    svcs_in_maintenance_id UUID NOT NULL,

    -- unique id for each row
    id UUID NOT NULL,

    -- FMRI of the SMF service in maintenance
    fmri TEXT,

    -- zone the SMF service in maintenance is located in
    zone TEXT,

    PRIMARY KEY (svcs_in_maintenance_id, id)
);