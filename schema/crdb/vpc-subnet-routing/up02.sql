/*
 * version information used to trigger VPC router RPW.
 * this is sensitive to CRUD on named resources beyond
 * routers e.g. instances, subnets, ...
 */
ALTER TABLE omicron.public.vpc_router
ADD COLUMN IF NOT EXISTS resolved_version INT NOT NULL DEFAULT 0;
