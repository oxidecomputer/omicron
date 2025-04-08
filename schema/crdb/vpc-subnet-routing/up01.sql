-- Each subnet may have a custom router attached.
ALTER TABLE omicron.public.vpc_subnet
ADD COLUMN IF NOT EXISTS custom_router_id UUID;
