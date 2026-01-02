ALTER TYPE
  omicron.public.user_provision_type
ADD VALUE IF NOT EXISTS
  'scim'
AFTER
  'jit'
;
