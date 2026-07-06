CREATE INDEX IF NOT EXISTS lookup_alerts_for_fm_case
ON omicron.public.alert (
    case_id
);
