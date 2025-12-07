# Bussig
Postgres Message Bus for .NET

```postgresql
bussig.metadata (
    queue_id    UUID PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    dead_letter_queue_id UUID NOT NULL REFERENCES queues(queue_id) --possibly
    created_at,
    modified_at
    -- possible per-queue settings
    -- max_deliver_attempts, message_ttl,lock_timeout, backoff
)
bussig.queue_{name} (
    id              bigserial primary key,
    payload         bytea not null,
    headers         jsonb not null default '{}'::jsonb,
    enqueued_at     timestamptz not null default now(),
    visible_at      timestamptz not null default now(),
    delivery_count  int not null default 0,
    
    lock_token      uuid null,
    lock_until      timestamptz null default null,
    correlation_id  uuid null,
    message_version int not null default 0
)
```

- Queues, dead letter queues
- Manual or automatic queue name/topic creation
- Ability to delay re-processing of message when abandoning. Backoff setting.
- Topics/subscription and DL. One subscriptions table holds topic and subscribers. One subscription events table holds topic, subscriber and payload
```postgresql
-- pseudo tables
-- What subscribes to what
subscriptions (
  id              bigserial primary key,
  topic_name      text not null,
  subscriber_name text not null,
  unique(topic_name, subscriber_name)
);

-- Actual messages, one table for *all* subscribers
subscription_events (
  id              bigserial primary key,
  topic_name      text not null,
  subscriber_name text not null,
  payload         jsonb not null,
  created_at      timestamptz not null default now(),
  -- plus retry / dead-letter metadata etc.
);
```
- Possibly async fan out (one insert into topic outbox)
- Polling: poll messages, poll health check. Option for using notify/listen?
- Request/reply
- Support for pgbouncer (transaction mode vs prepared statements)
- Inbox/outbox
- Tenants?
- OpenTelemetry
- Priority queueing?

Post-MVP
- Awesome saga support, MassTransit-esque
- Attachment support (blobs, no storing of files in pg). Compression?