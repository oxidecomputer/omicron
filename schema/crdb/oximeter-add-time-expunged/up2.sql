CREATE UNIQUE INDEX IF NOT EXISTS list_non_expunged_oximeter ON omicron.public.oximeter (
    id
) WHERE
    time_expunged IS NULL;
