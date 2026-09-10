-- Real-time event + notification platform. Own database. Not the workflow engine.

CREATE TABLE IF NOT EXISTS ntf_events (
  id               uuid PRIMARY KEY,
  tenant           text NOT NULL,
  event_type       text NOT NULL,
  idempotency_key  text NOT NULL,
  payload          jsonb NOT NULL,
  fanout_status    text NOT NULL DEFAULT 'complete',
  fanout_done      int NOT NULL DEFAULT 0,
  fanout_total     int NOT NULL DEFAULT 0,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant, event_type, idempotency_key),
  CONSTRAINT ntf_events_fanout_chk CHECK (
    fanout_status IN ('complete', 'pending', 'running')
  )
);

CREATE TABLE IF NOT EXISTS ntf_fanout (
  event_id  uuid NOT NULL REFERENCES ntf_events (id) ON DELETE CASCADE,
  tenant    text NOT NULL,
  user_id   text NOT NULL,
  seq       int  NOT NULL,
  status    text NOT NULL DEFAULT 'pending',
  PRIMARY KEY (event_id, user_id),
  CONSTRAINT ntf_fanout_status_chk CHECK (status IN ('pending', 'done', 'muted'))
);

CREATE TABLE IF NOT EXISTS ntf_inbox (
  id          bigserial PRIMARY KEY,
  tenant      text NOT NULL,
  user_id     text NOT NULL,
  event_id    uuid NOT NULL REFERENCES ntf_events (id),
  event_type  text NOT NULL,
  body        jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ntf_inbox_user_cursor
  ON ntf_inbox (tenant, user_id, id);

CREATE TABLE IF NOT EXISTS ntf_prefs (
  tenant      text NOT NULL,
  user_id     text NOT NULL,
  event_type  text NOT NULL,
  muted       boolean NOT NULL DEFAULT true,
  PRIMARY KEY (tenant, user_id, event_type)
);

CREATE TABLE IF NOT EXISTS ntf_deliveries (
  id              uuid PRIMARY KEY,
  event_id        uuid NOT NULL REFERENCES ntf_events (id),
  inbox_id        bigint,
  channel         text NOT NULL,
  destination     text NOT NULL,
  status          text NOT NULL,
  attempt_count   int NOT NULL DEFAULT 0,
  max_attempts    int NOT NULL,
  run_after       timestamptz NOT NULL DEFAULT now(),
  leased_until    timestamptz,
  worker_id       text,
  fencing_token   bigint NOT NULL DEFAULT 0,
  last_error      text,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ntf_deliveries_status_chk CHECK (
    status IN ('pending', 'running', 'succeeded', 'dead_lettered')
  )
);

CREATE INDEX IF NOT EXISTS ntf_deliveries_claimable
  ON ntf_deliveries (status, run_after, leased_until);

CREATE TABLE IF NOT EXISTS ntf_endpoints (
  tenant          text PRIMARY KEY,
  webhook_url     text NOT NULL,
  webhook_secret  text NOT NULL
);

CREATE TABLE IF NOT EXISTS ntf_lab_email (
  id          bigserial PRIMARY KEY,
  tenant      text NOT NULL,
  user_id     text NOT NULL,
  event_id    uuid NOT NULL,
  body        jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
