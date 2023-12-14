-- Preemptively turn any former fleet defaults into non-defaults if there's
-- going to be a silo conflicting with it after up6
UPDATE omicron.public.ip_pool_resource AS ipr
SET is_default = false
FROM omicron.public.ip_pool as ip
WHERE ipr.resource_type = 'silo' -- technically unnecessary because there is only silo
  AND ipr.is_default = true
  AND ip.is_default = true -- both being default is the conflict being resolved
  AND ip.silo_id IS NOT NULL
  AND ip.silo_id = ipr.resource_id
  AND ip.id = ipr.ip_pool_id;

