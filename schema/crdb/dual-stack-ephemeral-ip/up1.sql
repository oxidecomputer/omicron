-- Drop the existing ephemeral IP uniqueness constraint, which only allows
-- one ephemeral IP per instance. We need to replace it with a per-version
-- constraint to support dual-stack (one IPv4 + one IPv6 ephemeral IP).
DROP INDEX IF EXISTS one_ephemeral_ip_per_instance;
