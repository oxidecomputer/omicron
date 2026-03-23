-- Multicast group support: Add multicast groups and membership (RFD 488)

-- Create versioning sequence for multicast group changes
CREATE SEQUENCE IF NOT EXISTS omicron.public.multicast_group_version START 1 INCREMENT 1;
