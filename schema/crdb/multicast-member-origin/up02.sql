ALTER TABLE omicron.public.multicast_group_member
  ADD COLUMN IF NOT EXISTS membership_origin omicron.public.multicast_group_member_origin NOT NULL DEFAULT 'static';
