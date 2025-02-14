-- Add an FK to `vpc_subnet` for `vpc_subnet` routes.
ALTER TABLE omicron.public.router_route
ADD COLUMN IF NOT EXISTS vpc_subnet_id UUID;
