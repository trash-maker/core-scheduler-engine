-- =====================================================================
-- Signal-driven notification engine - schema v1
-- PostgreSQL is the source of truth. All timestamps are timestamptz (UTC).
-- =====================================================================

-- ---------------------------------------------------------------------
-- Signals
-- ---------------------------------------------------------------------
-- Idempotency on signal_id (per tenant). Short retention (ops job).
CREATE TABLE signal_inbox (
    tenant_id    text        NOT NULL,
    signal_id    text        NOT NULL,
    received_at  timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, signal_id)
);
CREATE INDEX signal_inbox_received_idx ON signal_inbox (received_at);

-- Latest value per (tenant, correlation_key, signal). Updated only if occurred_at is newer.
CREATE TABLE signal_state (
    tenant_id       text        NOT NULL,
    correlation_key text        NOT NULL,
    signal          text        NOT NULL,
    kind            text        NOT NULL CHECK (kind IN ('KPI', 'DATE')),
    value           jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    received_at     timestamptz NOT NULL,
    signal_id       text        NOT NULL,
    PRIMARY KEY (tenant_id, correlation_key, signal)
);
-- Entity sweeps for time-based rules (keyset over correlation_key for a set of signals).
CREATE INDEX signal_state_by_signal_idx ON signal_state (tenant_id, signal, correlation_key);

-- ---------------------------------------------------------------------
-- Rules
-- ---------------------------------------------------------------------
CREATE SEQUENCE rule_change_seq;

CREATE TABLE rule (
    tenant_id   text        NOT NULL,
    rule_id     text        NOT NULL,
    version     int         NOT NULL,
    enabled     boolean     NOT NULL,
    current     boolean     NOT NULL,
    definition  jsonb       NOT NULL,
    change_seq  bigint      NOT NULL DEFAULT nextval('rule_change_seq'),
    created_at  timestamptz NOT NULL,
    created_by  text,
    PRIMARY KEY (tenant_id, rule_id, version)
);
CREATE UNIQUE INDEX rule_current_uq ON rule (tenant_id, rule_id) WHERE current;
CREATE INDEX rule_change_seq_idx ON rule (change_seq);

CREATE TABLE rule_state (
    tenant_id         text        NOT NULL,
    rule_id           text        NOT NULL,
    correlation_key   text        NOT NULL,
    last_result       boolean     NOT NULL DEFAULT false,
    edge_seq          bigint      NOT NULL DEFAULT 0,
    last_fired_at     timestamptz,
    last_evaluated_at timestamptz,
    rule_version      int,
    leaf_state        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, rule_id, correlation_key)
);

-- ---------------------------------------------------------------------
-- Notifications (range partitioned by created_at; id is UUIDv7 whose
-- embedded timestamp == created_at, so lookups by id prune partitions)
-- ---------------------------------------------------------------------
CREATE TABLE notification (
    id                uuid        NOT NULL,
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL,
    tenant_id         text        NOT NULL,
    rule_id           text        NOT NULL,
    rule_version      int         NOT NULL,
    template_id       text        NOT NULL,
    correlation_key   text        NOT NULL,
    action            text        NOT NULL,
    type              text        NOT NULL,
    status            text        NOT NULL,
    status_reason     text,
    attempt           int         NOT NULL DEFAULT 0,   -- monotonic attempt number sent to actuators
    cycle_attempts    int         NOT NULL DEFAULT 0,   -- attempts in current retry cycle (reset by re-trigger)
    retry_policy      jsonb       NOT NULL,
    body              jsonb,                            -- resolved, immutable once set
    spec              jsonb       NOT NULL,             -- immutable template snapshot needed later (parent cfg, $parent body)
    context           jsonb,                            -- frozen resolution context (signals snapshot, trigger) for deferred parts
    due_at            timestamptz,
    expires_at        timestamptz,
    attempt_timer_id  bigint,
    version           int         NOT NULL DEFAULT 0,
    idempotency_key   text        NOT NULL,
    parent_id         uuid,
    parent_trigger    text,
    depth             int         NOT NULL DEFAULT 0,
    series_id         uuid,
    occurrence_index  int,
    cancel_on         text[]      NOT NULL DEFAULT '{}',
    failed_once       boolean     NOT NULL DEFAULT false,
    last_failure      jsonb,
    published_at      timestamptz,
    terminal_at       timestamptz,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX notification_tenant_id_idx     ON notification (tenant_id, id);
CREATE INDEX notification_tenant_key_idx    ON notification (tenant_id, correlation_key, id);
CREATE INDEX notification_tenant_status_idx ON notification (tenant_id, status, id);
CREATE INDEX notification_tenant_due_idx    ON notification (tenant_id, due_at, id);
CREATE INDEX notification_parent_idx        ON notification (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX notification_cancelable_idx    ON notification (tenant_id, correlation_key)
    WHERE cardinality(cancel_on) > 0
      AND status IN ('WAITING_ON_PARENT', 'CREATED', 'SCHEDULED', 'PUBLISHED', 'RETRY_SCHEDULED');
CREATE INDEX notification_terminal_idx      ON notification (terminal_at) WHERE terminal_at IS NOT NULL;

CREATE SEQUENCE notification_history_seq;
CREATE TABLE notification_history (
    id                      bigint      NOT NULL DEFAULT nextval('notification_history_seq'),
    notification_id         uuid        NOT NULL,
    notification_created_at timestamptz NOT NULL,
    tenant_id               text        NOT NULL,
    at                      timestamptz NOT NULL,
    from_status             text,
    to_status               text        NOT NULL,
    event                   text        NOT NULL,
    attempt                 int,
    reason                  text,
    PRIMARY KEY (id, notification_created_at)
) PARTITION BY RANGE (notification_created_at);
CREATE INDEX notification_history_nid_idx ON notification_history (notification_id, id);

-- Idempotency of notification creation: (rule, key, edge_seq/instant, template). Short retention.
CREATE TABLE notification_dedup (
    idempotency_key text        PRIMARY KEY,
    notification_id uuid        NOT NULL,
    created_at      timestamptz NOT NULL
);
CREATE INDEX notification_dedup_created_idx ON notification_dedup (created_at);

-- ---------------------------------------------------------------------
-- Recurring series (top-level RECURRING templates and WHILE_IN_STATE children)
-- ---------------------------------------------------------------------
CREATE TABLE recurring_series (
    id               uuid        PRIMARY KEY,
    tenant_id        text        NOT NULL,
    rule_id          text        NOT NULL,
    rule_version     int         NOT NULL,
    template_id      text        NOT NULL,
    correlation_key  text        NOT NULL,
    action           text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('WAITING', 'ACTIVE', 'CLOSED')),
    close_reason     text,
    parent_id        uuid,
    spec             jsonb       NOT NULL,
    context          jsonb       NOT NULL,
    start_at         timestamptz,
    until_at         timestamptz,
    max_occurrences  int,
    occurrences      int         NOT NULL DEFAULT 0,
    skipped          int         NOT NULL DEFAULT 0,
    next_due_at      timestamptz,
    cancel_on        text[]      NOT NULL DEFAULT '{}',
    idempotency_key  text        NOT NULL,
    version          int         NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);
CREATE UNIQUE INDEX recurring_series_idem_uq ON recurring_series (idempotency_key);
-- one active series per (parent, template)
CREATE UNIQUE INDEX recurring_series_one_active_uq ON recurring_series (parent_id, template_id)
    WHERE status = 'ACTIVE' AND parent_id IS NOT NULL;
CREATE INDEX recurring_series_parent_idx ON recurring_series (parent_id) WHERE parent_id IS NOT NULL AND status <> 'CLOSED';
CREATE INDEX recurring_series_cancel_idx ON recurring_series (tenant_id, correlation_key)
    WHERE status <> 'CLOSED' AND cardinality(cancel_on) > 0;
CREATE INDEX recurring_series_closed_idx ON recurring_series (updated_at) WHERE status = 'CLOSED';

-- ---------------------------------------------------------------------
-- Timers: drive everything time based. Claimed with DELETE ... SKIP LOCKED RETURNING.
-- ---------------------------------------------------------------------
CREATE TABLE timer (
    id               bigserial   PRIMARY KEY,
    kind             text        NOT NULL CHECK (kind IN ('PUBLISH', 'ATTEMPT_TIMEOUT', 'EXPIRE', 'NEXT_OCCURRENCE', 'RULE_EVAL')),
    due_at           timestamptz NOT NULL,
    tenant_id        text        NOT NULL,
    notification_id  uuid,
    series_id        uuid,
    rule_id          text,
    correlation_key  text,
    payload          jsonb,
    failures         int         NOT NULL DEFAULT 0,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX timer_due_idx ON timer (due_at);
CREATE UNIQUE INDEX timer_rule_eval_uq ON timer (tenant_id, rule_id, correlation_key) WHERE kind = 'RULE_EVAL';
CREATE INDEX timer_notification_idx ON timer (notification_id) WHERE notification_id IS NOT NULL;
CREATE INDEX timer_series_idx ON timer (series_id) WHERE series_id IS NOT NULL;
-- high churn queue table: aggressive autovacuum, room for HOT updates
ALTER TABLE timer SET (autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 5000,
                       autovacuum_vacuum_cost_delay = 0, autovacuum_analyze_scale_factor = 0.02, fillfactor = 90);

CREATE TABLE timer_dead (
    id               bigint      PRIMARY KEY,
    kind             text        NOT NULL,
    due_at           timestamptz NOT NULL,
    tenant_id        text        NOT NULL,
    notification_id  uuid,
    series_id        uuid,
    rule_id          text,
    correlation_key  text,
    payload          jsonb,
    failures         int         NOT NULL,
    last_error       text,
    created_at       timestamptz NOT NULL,
    died_at          timestamptz NOT NULL
);
CREATE INDEX timer_dead_died_idx ON timer_dead (died_at);

-- ---------------------------------------------------------------------
-- Transactional outbox (rows deleted after Kafka ack)
-- ---------------------------------------------------------------------
CREATE TABLE outbox (
    id                 bigserial   PRIMARY KEY,
    topic              text        NOT NULL,
    kafka_key          text        NOT NULL,
    headers            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    payload            text        NOT NULL,
    tenant_id          text        NOT NULL,
    notification_id    uuid,
    attempt            int,
    attempt_timeout_ms bigint,
    created_at         timestamptz NOT NULL
);
ALTER TABLE outbox SET (autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 5000,
                        autovacuum_vacuum_cost_delay = 0, autovacuum_analyze_scale_factor = 0.02);

-- ---------------------------------------------------------------------
-- Dead letters (also published to the DLQ topic through the outbox)
-- ---------------------------------------------------------------------
CREATE TABLE dead_letter (
    id          bigserial   PRIMARY KEY,
    source      text        NOT NULL,
    tenant_id   text,
    reason      text        NOT NULL,
    detail      text,
    payload     text,
    created_at  timestamptz NOT NULL
);
CREATE INDEX dead_letter_created_idx ON dead_letter (created_at);

-- ---------------------------------------------------------------------
-- Partition maintenance: monthly partitions for notification + history
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ensure_monthly_partitions(p_from timestamptz, p_months_ahead int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    t      text;
    m      int;
    lo     timestamptz;
    hi     timestamptz;
    pname  text;
    made   int := 0;
BEGIN
    FOREACH t IN ARRAY ARRAY['notification', 'notification_history'] LOOP
        FOR m IN -1 .. p_months_ahead LOOP
            lo := date_trunc('month', p_from AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + make_interval(months => m);
            hi := lo + interval '1 month';
            pname := format('%s_p%s', t, to_char(lo AT TIME ZONE 'UTC', 'YYYY_MM'));
            IF to_regclass(pname) IS NULL THEN
                EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)', pname, t, lo, hi);
                made := made + 1;
            END IF;
        END LOOP;
    END LOOP;
    RETURN made;
END $$;

SELECT ensure_monthly_partitions(now(), 3);
