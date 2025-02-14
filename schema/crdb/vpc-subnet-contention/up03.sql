-- Ensure 'vpc_subnet's are the only rules with FKs for now.
ALTER TABLE omicron.public.router_route
ADD CONSTRAINT IF NOT EXISTS non_null_vpc_subnet CHECK (
    (kind = 'vpc_subnet' AND vpc_subnet_id IS NOT NULL) OR
    (kind != 'vpc_subnet' AND vpc_subnet_id IS NULL)
);
