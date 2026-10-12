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

    -- which slot in the power shelf this record represents. this is determined
    -- based on the SP component ID reported by the SP's inventory response.
    location omicron.public.inv_psu_slot NOT NULL,

    -- the SP-reported presence value for this PSU.
    presence omicron.public.sp_component_presence NOT NULL,

    -- the Hubris device type string reported by the SP's inventory.
    --
    -- this identifies which Hubris driver is used to communicate with the PSU,
    -- and is a property of the SP's Hubris image, not a value reported by the
    -- PSU itself.
    hubris_device_type TEXT NOT NULL,

    -- PMBus vital product data reported by the PSU. these fields are present
    -- when the VPD was collected successfully, and are null if it was not, or
    -- if the PSU is not present.
    mfr_id TEXT,
    mfr_model TEXT,
    mfr_location TEXT,
    -- the value of the PMBus "MFR_REVISION" command. This is used by Murata to
    -- represent the PSU's MCU firmware revisions, which are parsed into the
    -- `primary_fw_rev` and `secondary_fw_rev` fields.
    --
    -- see: https://github.com/oxidecomputer/hubris/blob/929f0d3709025c33cd3ad51c09df6c2fbb55047d/drv/i2c-devices/src/mwocp6x.rs#L202-L209
    mfr_revision TEXT,
    mfr_date TEXT,
    mfr_serial TEXT,

    -- the firmware revision of the primary MCU (AC input side), as defined in
    -- ACAN-114 and ACAN-157.
    primary_fw_rev STRING(4),
    -- the firmware revision of the secondary MCU (DC output side), as defined in
    -- ACAN-114 and ACAN-157.
    secondary_fw_rev STRING(4),

    -- an error that occurred while reading PMBus VPD. this is NULL if the VPD
    -- fields are present or if the PSU is not present, and is set if the
    -- VPD fields are NULL because an attempt to read the VPD failed.
    vpd_error TEXT,

    -- an error that occurred while parsing the PSU's firmware revision from the
    -- `mfr_revision` field. this is NULL if the firmware revisions were parsed
    -- successfully, or if there is no `mfr_revision` to parse them from.
    fw_rev_error TEXT,

    CONSTRAINT vpd_result_valid CHECK (
        (
            presence != 'not_present'
            AND vpd_error IS NULL
            AND mfr_id IS NOT NULL
            AND mfr_model IS NOT NULL
            AND mfr_revision IS NOT NULL
            AND mfr_location IS NOT NULL
            AND mfr_date IS NOT NULL
            AND mfr_serial IS NOT NULL
        ) OR (
            presence != 'not_present'
            AND vpd_error IS NOT NULL
            AND mfr_id IS NULL
            AND mfr_model IS NULL
            AND mfr_revision IS NULL
            AND mfr_location IS NULL
            AND mfr_date IS NULL
            AND mfr_serial IS NULL
        ) OR (
            presence = 'not_present'
            AND vpd_error IS NULL
            AND mfr_id IS NULL
            AND mfr_model IS NULL
            AND mfr_revision IS NULL
            AND mfr_location IS NULL
            AND mfr_date IS NULL
            AND mfr_serial IS NULL
        )
    ),

    -- firmware revisions are parsed from `mfr_revision`, so they (or an error
    -- parsing them) are present if and only if `mfr_revision` is. together
    -- with `vpd_result_valid`, this distinguishes a PSU whose VPD was read
    -- successfully but whose firmware revisions could not be parsed from a PSU
    -- whose VPD could not be read at all.
    CONSTRAINT fw_rev_result_valid CHECK (
        (
            mfr_revision IS NOT NULL
            AND primary_fw_rev IS NOT NULL
            AND secondary_fw_rev IS NOT NULL
            AND fw_rev_error IS NULL
        ) OR (
            mfr_revision IS NOT NULL
            AND primary_fw_rev IS NULL
            AND secondary_fw_rev IS NULL
            AND fw_rev_error IS NOT NULL
        ) OR (
            mfr_revision IS NULL
            AND primary_fw_rev IS NULL
            AND secondary_fw_rev IS NULL
            AND fw_rev_error IS NULL
        )
    ),

    PRIMARY KEY (inv_collection_id, psc_baseboard_id, location)
);
