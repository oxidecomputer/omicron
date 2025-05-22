ALTER TABLE omicron.public.vpc_firewall_rule
  ADD COLUMN IF NOT EXISTS filter_protocols STRING(32)[];
