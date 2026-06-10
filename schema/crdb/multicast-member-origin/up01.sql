CREATE TYPE IF NOT EXISTS omicron.public.multicast_group_member_origin AS ENUM (
  'static',
  'igmp_snooped'
);
