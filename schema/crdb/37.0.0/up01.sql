CREATE TYPE IF NOT EXISTS omicron.public.live_repair_notification_type AS ENUM (
  'started',
  'succeeded',
  'failed'
);
