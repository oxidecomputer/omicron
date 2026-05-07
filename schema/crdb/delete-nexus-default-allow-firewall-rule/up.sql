/*
 * We're changing the default firewall rule for Nexus specifically. Instead of a
 * default-allow (on top of OPTE's builtin default-deny), we add nothing
 * special. Nexus's public API server then defaults to unavailable, until the
 * allow-list plumbing is propagated (by Nexus's background task) which adds a
 * rule allowing traffic from the desired sources.
 */
UPDATE omicron.public.vpc_firewall_rule
SET time_deleted = now()
WHERE name = 'nexus-inbound'
AND vpc_id = '001de000-074c-4000-8000-000000000000'
AND time_deleted IS NULL;
