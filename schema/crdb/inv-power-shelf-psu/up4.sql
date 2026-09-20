-- inventory table for power supply units (PSUs) in a power shelf
CREATE TABLE IF NOT EXISTS omicron.public.inv_power_shelf_psu (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,
    -- baseboard of the power shelf controller which told us about this PSU
    -- (foreign key into `hw_baseboard_id` table)
    psc_baseboard_id UUID NOT NULL,
    -- which slot in the power shelf this record represents
    location omicron.public.inv_psu_slot NOT NULL,
    -- the SP-reported presence value for this PSU
    presence omicron.public.sp_component_presence NOT NULL,

    -- PSU device reported by Hubris
    device omicron.public.inv_psu_device NOT NULL,

    -- PMBus vital product data reported by the PSU. information reported by the
    -- PSU. these fields are present when the VPD was collected successfully,
    -- and are null if it was not.
    mfr_id TEXT,
    mfr_model TEXT,
    firmware_rev TEXT,
    mfr_location TEXT,
    mfr_date TEXT,
    mfr_serial TEXT,

    -- an error that occurred while reading PMBus VPD. this is NULL if the VPD
    -- fields are present, and is present if the VPD is NULL.
    vpd_error TEXT,

    CONSTRAINT vpd_result_valid CHECK (
        (
            vpd_error IS NULL
            AND mfr_id IS NOT NULL
            AND mfr_model IS NOT NULL
            AND firmware_rev IS NOT NULL
            AND mfr_location IS NOT NULL
            AND mfr_date IS NOT NULL
            AND mfr_serial IS NOT NULL
        ) OR (
            vpd_error IS NOT NULL
            AND mfr_id IS NULL
            AND mfr_model IS NULL
            AND firmware_rev IS NULL
            AND mfr_location IS NULL
            AND mfr_date IS NULL
            AND mfr_serial IS NULL
        )
    ),

    PRIMARY KEY (inv_collection_id, psc_baseboard_id, location)
);
