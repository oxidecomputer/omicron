-- Enforce a limit of one ephemeral IP per IP version per instance.
-- This allows dual-stack configurations with one IPv4 and one IPv6 ephemeral IP.
-- Uses family(ip::INET) to determine the IP version (4 or 6) from the address.
CREATE UNIQUE INDEX IF NOT EXISTS one_ephemeral_ip_per_instance_per_version
ON omicron.public.external_ip (parent_id, (family(ip::INET)))
WHERE kind = 'ephemeral' AND parent_id IS NOT NULL AND time_deleted IS NULL;
