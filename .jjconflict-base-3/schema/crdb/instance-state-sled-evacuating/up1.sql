ALTER TYPE
  omicron.public.instance_state_v2
ADD VALUE IF NOT EXISTS
  'sled_evacuating'
AFTER
  'destroyed'
;
