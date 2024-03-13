CREATE TYPE IF NOT EXISTS omicron.public.downstairs_client_stop_request_reason_type AS ENUM (
  'replacing',
  'disabled',
  'failed_reconcile',
  'io_error',
  'bad_negotiation_order',
  'incompatible',
  'failed_live_repair',
  'too_many_outstanding_jobs',
  'deactivated'
);
