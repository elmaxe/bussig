# Bussig
Postgres Message Bus for .NET

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
- Serialization support (System.Text.Json, Newtonsoft.Json, custom), (encrypt messages aes)
- Message versioning
- cron jobs, quartz, possibly pgcron
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