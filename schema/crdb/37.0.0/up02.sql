CREATE TABLE IF NOT EXISTS live_repair_notification (
    time TIMESTAMPTZ NOT NULL,

    repair_id UUID NOT NULL,
    upstairs_id UUID NOT NULL,
    session_id UUID NOT NULL,

    region_id UUID NOT NULL,
    target_ip INET NOT NULL,
    target_port INT4 CHECK (target_port BETWEEN 0 AND 65535) NOT NULL,

    notification_type omicron.public.live_repair_notification_type NOT NULL,

    /*
     * A live repair is uniquely identified by the four UUIDs here, and a
     * notification is uniquely identified by its type.
     */
    PRIMARY KEY (repair_id, upstairs_id, session_id, region_id, notification_type)
);
