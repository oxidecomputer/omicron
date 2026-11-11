ALTER TYPE
  omicron.public.inv_svc_enabled_not_online_state
ADD VALUE IF NOT EXISTS
  'unrecognized'
AFTER
  'maintenance'
;
