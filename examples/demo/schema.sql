-- Demo schema for dryrun
--
-- Naive/small app with deliberate schema issues to demostrate dryrun

BEGIN;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-------------------------------------------------------------------
-- organizations (formerly teams)
--
-- Modern SaaS multi-tenancy with external_id and slug.
-------------------------------------------------------------------

CREATE TABLE organizations (
    organization_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id     uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    name            text NOT NULL,
    slug            text NOT NULL UNIQUE,
    settings        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-------------------------------------------------------------------
-- users
--
-- varchar(255) on email: a Rails/Django habit that dryrun flags.
-- Missing updated_at.
-------------------------------------------------------------------

CREATE TABLE users (
    user_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email      varchar(255) NOT NULL UNIQUE,
    name       text NOT NULL,
    organization_id bigint REFERENCES organizations (organization_id),
    created_at timestamptz NOT NULL DEFAULT now()
);

-------------------------------------------------------------------
-- projects
--
-- serial instead of identity - the old way.
-- timestamp without time zone - loses timezone context.
-------------------------------------------------------------------

CREATE TABLE projects (
    project_id serial PRIMARY KEY,
    organization_id bigint NOT NULL REFERENCES organizations (organization_id),
    name       text NOT NULL,
    status     text NOT NULL DEFAULT 'active',
    metadata   jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE INDEX projects_by_organization ON projects (organization_id);

-------------------------------------------------------------------
-- tasks
--
-- Foreign keys on project_id and assignee_id without indexes.
-- Unnamed CHECK constraint on status.
-------------------------------------------------------------------

CREATE TABLE tasks (
    task_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id  bigint NOT NULL REFERENCES projects (project_id),
    assignee_id bigint REFERENCES users (user_id),
    title       text NOT NULL,
    status      text NOT NULL DEFAULT 'open',
    priority    integer NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CHECK (status IN ('open', 'in_progress', 'done', 'cancelled'))
);

-------------------------------------------------------------------
-- task_comments (formerly comments)
--
-- No primary key.
-------------------------------------------------------------------

CREATE TABLE task_comments (
    task_id    bigint NOT NULL REFERENCES tasks (task_id),
    user_id    bigint NOT NULL REFERENCES users (user_id),
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX task_comments_by_task ON task_comments (task_id);
CREATE INDEX task_comments_by_user ON task_comments (user_id);

-------------------------------------------------------------------
-- tags / task_tags
-------------------------------------------------------------------

CREATE TABLE tags (
    tag_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE task_tags (
    task_id bigint NOT NULL REFERENCES tasks (task_id),
    tag_id  bigint NOT NULL REFERENCES tags (tag_id),
    PRIMARY KEY (task_id, tag_id)
);

-------------------------------------------------------------------
-- audit_log (partitioned)
--
-- Range-partitioned by created_at. Q3 2024 is missing - a gap
-- that will cause INSERT failures for July–September dates.
-- No DEFAULT partition.
-------------------------------------------------------------------

CREATE TABLE audit_log (
    log_id     bigint GENERATED ALWAYS AS IDENTITY,
    user_id    bigint,
    action     text NOT NULL,
    detail     text,
    detail_meta jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE TABLE audit_log_2024q1 PARTITION OF audit_log
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE audit_log_2024q2 PARTITION OF audit_log
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
-- Q3 intentionally missing
CREATE TABLE audit_log_2024q4 PARTITION OF audit_log
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE audit_log_2025q1 PARTITION OF audit_log
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');

-------------------------------------------------------------------
-- user_notifications (formerly notifications)
--
-- varchar columns where text would do. No created_at, no updated_at.
-- Duplicate index on user_id under two names.
-------------------------------------------------------------------

CREATE TABLE user_notifications (
    notification_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         bigint NOT NULL REFERENCES users (user_id),
    channel         varchar(50) NOT NULL DEFAULT 'email',
    message         varchar(1000) NOT NULL,
    read            boolean NOT NULL DEFAULT false
);

CREATE INDEX user_notifications_by_user ON user_notifications (user_id);
CREATE INDEX user_notifications_user_lookup ON user_notifications (user_id);

COMMIT;

-------------------------------------------------------------------
-- Seed data
-------------------------------------------------------------------

INSERT INTO organizations (name, slug, settings)
VALUES
    ('Engineering', 'engineering', '{"tier": "enterprise", "seats": 50}'::jsonb),
    ('Product', 'product', '{"tier": "pro", "seats": 20}'::jsonb),
    ('Design', 'design', '{"tier": "pro", "seats": 15}'::jsonb);

INSERT INTO users (email, name, organization_id)
SELECT
    format('user%s@example.com', n),
    format('User %s', n),
    1 + (n % 3)
FROM generate_series(1, 50) AS s(n);

INSERT INTO projects (organization_id, name, status, metadata)
SELECT
    1 + (n % 3),
    format('Project %s', n),
    CASE WHEN n % 5 = 0 THEN 'archived' ELSE 'active' END,
    jsonb_build_object('priority', CASE WHEN n % 3 = 0 THEN 'high' ELSE 'normal' END, 'budget', n * 1000)
FROM generate_series(1, 10) AS s(n);

INSERT INTO tasks (project_id, assignee_id, title, status, priority)
SELECT
    1 + (n % 10),
    1 + (n % 50),
    format('Task %s', n),
    (ARRAY['open', 'in_progress', 'done'])[1 + (n % 3)],
    n % 4
FROM generate_series(1, 200) AS s(n);

INSERT INTO task_comments (task_id, user_id, body)
SELECT
    1 + (n % 200),
    1 + (n % 50),
    format('Comment on task from user %s', n)
FROM generate_series(1, 500) AS s(n);

INSERT INTO tags (name)
VALUES ('bug'), ('feature'), ('urgent'), ('docs'), ('tech-debt');

INSERT INTO task_tags (task_id, tag_id)
SELECT DISTINCT 1 + (n % 200), 1 + (n % 5)
FROM generate_series(1, 300) AS s(n);

INSERT INTO audit_log (user_id, action, detail, detail_meta, created_at)
SELECT
    1 + (n % 50),
    (ARRAY['login', 'create', 'update', 'delete'])[1 + (n % 4)],
    format('Action detail %s', n),
    jsonb_build_object('ip', format('10.0.%s.%s', n % 256, (n * 7) % 256), 'source', CASE WHEN n % 2 = 0 THEN 'web' ELSE 'api' END),
    '2024-01-15'::timestamptz + make_interval(hours => n)
FROM generate_series(1, 1000) AS s(n);

INSERT INTO user_notifications (user_id, channel, message)
SELECT
    1 + (n % 50),
    CASE WHEN n % 3 = 0 THEN 'slack' ELSE 'email' END,
    format('Notification %s', n)
FROM generate_series(1, 100) AS s(n);

ANALYZE;
