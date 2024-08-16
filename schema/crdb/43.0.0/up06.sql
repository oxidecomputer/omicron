CREATE TABLE IF NOT EXISTS omicron.public.downstairs_client_stop_request_notification (
    time TIMESTAMPTZ NOT NULL,
    upstairs_id UUID NOT NULL,
    downstairs_id UUID NOT NULL,
    reason omicron.public.downstairs_client_stop_request_reason_type NOT NULL,

    PRIMARY KEY (time, upstairs_id, downstairs_id, reason)
);
