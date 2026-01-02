-- Add a well-known rule to services VPC to allow limited forms
-- of ICMP traffic. Inserting this rule is conditional on the
-- 'oxide-services' VPC existing.
INSERT INTO omicron.public.vpc_firewall_rule (
  id,
  name, description,
  time_created, time_modified, vpc_id, status,
  targets,
  filter_protocols,
  direction, action, priority
)
SELECT
  gen_random_uuid(),
  -- Hardcoded name/description, see nexus/db-fixed-data/src/vpc_firewall_rule.rs.
  'nexus-icmp', 'allow typical inbound ICMP error codes for outbound flows',
  NOW(), NOW(), vpc.id, 'enabled',
  -- Apply to the Nexus and External DNS zones...
  ARRAY['subnet:nexus','subnet:external-dns'],
  -- Allow inbound ICMP:
  --  * Destination Unreachable
  --    * Port Unreachable
  --    * Fragmentation Needed
  --  * Redirect
  --  * Time Exceeded
  ARRAY['icmp:3,3-4','icmp:5','icmp:11'],
  'inbound', 'allow', 65534
FROM omicron.public.vpc
  WHERE vpc.id = '001de000-074c-4000-8000-000000000000'
    AND vpc.name = 'oxide-services'
ON CONFLICT DO NOTHING;
