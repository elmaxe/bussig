# Bussig
Postgres Message Bus for .NET

- Queues, dead letter queues
- Manual or automatic queue name/topic creation
- Ability to delay re-processing of message when abandoning. Backoff setting.
- Topics/subscription and DL
- Possibly async fan out (one insert into topic outbox)
- Serialization support (System.Text.Json, Newtonsoft.Json, custom), (encrypt messages aes)
- Message versioning
- cron jobs/schedules, quartz/hangfire
- Polling: poll messages, poll health check. Option for using notify/listen?
- Request/reply
- Support for pgbouncer (transaction mode vs prepared statements)
- Inbox/outbox
- Tenants?
- OpenTelemetry
- Priority queueing
- Message deduplication maybe? Otherwise idempotent, or use sagas instead
- A processor/consume context alternative that returns a message, the framework handles the sending of it, with outbox if configured
    - For example: `IConsumeMessage<TConsumeMessage>`, `IConsumeMessage<TConsumeMessage, TSendMessage>`, `IConsumeMessage<TConsumeMessage, IEnumerable<TSendMessage>>`
```csharp
public interface IConsumeMessage<TConsumeMessage> {
    public async Task Consume(TConsumeMessage message)
}
public interface IConsumeMessage<TConsumeMessage, TSendMessage> {
    public async Task<TSendMessage> Consume(TConsumeMessage message)
}
public interface IConsumeMessage<TConsumeMessage, IEnumerable<TSendMessage>> {
    public async Task<IEnumerable<TSendMessage>> Consume(TConsumeMessage message)
}
```

Post-MVP
- Awesome saga support, MassTransit-esque
- Attachment support (blobs, no storing of files in pg). Compression?