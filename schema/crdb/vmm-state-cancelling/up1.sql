ALTER TYPE
  omicron.public.vmm_state
ADD VALUE IF NOT EXISTS
  'cancelling'
AFTER
  'saga_unwound'
