CREATE TYPE IF NOT EXISTS omicron.public.downstairs_client_stopped_reason_type AS ENUM (
  'connection_timeout',
  'connection_failed',
  'timeout',
  'write_failed',
  'read_failed',
  'requested_stop',
  'finished',
  'queue_closed',
  'receive_task_cancelled'
);
