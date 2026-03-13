CREATE TABLE IF NOT EXISTS omicron.public.fm_alert_request (
    -- Requested alert UUID
    id UUID NOT NULL,
    -- UUID of the sitrep in which the alert is requested.
    sitrep_id UUID NOT NULL,
    -- UUID of the sitrep in which the alert request was created.
    requested_sitrep_id UUID NOT NULL,
    -- UUID of the case to which this alert request belongs.
    case_id UUID NOT NULL,

    -- The class of alert that was requested
    alert_class omicron.public.alert_class NOT NULL,
    -- Actual alert data. The structure of this depends on the alert class.
    payload JSONB NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
