CREATE TABLE IF NOT EXISTS omicron.public.fm_ereport_in_case (
    -- ID of this association. When an ereport is assigned to a case, that
    -- association is assigned a UUID. These are used primarily to aid in
    -- paginating queries to this table, which would otherwise require a
    -- three-column pagination utility in order to paginate by (case_id,
    -- restart_id, ena).
    id UUID NOT NULL,
    --  The ereport's identity.
    restart_id UUID NOT NULL,
    ena INT8 NOT NULL,

    -- UUID of the case the ereport is assigned to.
    case_id UUID NOT NULL,

    -- UUID of the sitrep in which this assignment exists.
    sitrep_id UUID NOT NULL,
    -- UUID of the sitrep in which the ereport was initially assigned to this
    -- case.
    assigned_sitrep_id UUID NOT NULL,

    comment TEXT NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
