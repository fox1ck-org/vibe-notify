-- Copy this into a migration in the owning service's own schema.
--
-- Each service keeps its own outbox rather than sharing one: the row must be
-- written in the same transaction as the state change it describes, and that
-- is only possible in the database that holds the state.

CREATE TABLE IF NOT EXISTS notification_outbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    envelope        jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','IN_PROGRESS','DONE','FAILED','DEAD')),
    attempts        int         NOT NULL DEFAULT 0,
    last_error      text        NOT NULL DEFAULT '',
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- The drainer's only query.
CREATE INDEX IF NOT EXISTS notification_outbox_drain_idx
    ON notification_outbox (next_attempt_at)
    WHERE status IN ('PENDING','FAILED');

-- What to alert on: a DEAD row is a notification that will never arrive, and
-- an old PENDING one means the service cannot reach the hub.
CREATE INDEX IF NOT EXISTS notification_outbox_dead_idx
    ON notification_outbox (updated_at)
    WHERE status = 'DEAD';
