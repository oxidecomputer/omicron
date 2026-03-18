CREATE TABLE IF NOT EXISTS omicron.public.fm_support_bundle_request (
    -- Requested support bundle UUID.
    id UUID NOT NULL,
    -- UUID of the current sitrep that this request record is part of.
    --
    -- Note that this is *not* the sitrep in which the bundle was requested.
    sitrep_id UUID NOT NULL,
    -- UUID of the original sitrep in which the bundle was first requested.
    requested_sitrep_id UUID NOT NULL,
    -- UUID of the case to which this request belongs.
    case_id UUID NOT NULL,
    -- Human-readable reason for requesting the bundle.
    reason TEXT NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
