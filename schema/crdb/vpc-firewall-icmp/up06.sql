-- Add a well-known rule to services VPC to allow limited forms
-- of ICMP traffic. Inserting this rule is conditional on the
-- '' VPC existing.
INSERT INTO omicron.public.vpc_firewall_rule (
  id,
  name, description,
  time_created, time_modified, vpc_id, status, direction,
  targets, filter_protocols, action, priority
)
SELECT
  gen_random_uuid(),
  -- Hardcoded name/description, see nexus/db-fixed-data/src/vpc_firewall_rule.rs.
  'nexus-icmp', 'allow typical inbound ICMP error codes for outbound flows',
  NOW(), NOW(), vpc.id, 'enabled', 'inbound',
  -- Allow inbound ICMP Destination Unreachable (Too Large) and Time Exceeded
  ARRAY['subnet:nexus'], ARRAY['icmp:3,4','icmp:11'], 'allow', 65534
FROM omicron.public.vpc
  WHERE vpc.id = '001de000-074c-4000-8000-000000000000'
    AND vpc.name = 'oxide-services'
ON CONFLICT DO NOTHING;
