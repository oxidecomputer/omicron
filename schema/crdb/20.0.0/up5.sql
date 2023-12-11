-- turn any former fleet defaults into non-defaults if there's going to be a
-- silo conflicting with it
UPDATE omicron.public.ip_pool_resource AS ipr
SET is_default = false
FROM omicron.public.ip_pool as ip
WHERE ipr.is_default = true
  AND ip.is_default = true -- both being default is the conflict being resolved
  AND ip.silo_id = ipr.resource_id
  AND ip.id = ipr.ip_pool_id;

