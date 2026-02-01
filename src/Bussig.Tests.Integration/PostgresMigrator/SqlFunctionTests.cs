#pragma warning disable CA1861 // Avoid constant arrays as arguments

using System.Globalization;
using System.Text.Json;
using Bussig.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using NpgsqlTypes;
using Testcontainers.PostgreSql;

namespace Bussig.Tests.Integration.PostgresMigrator;

public class SqlFunctionTests
{
    [ClassDataSource<PostgresContainerPool>(Shared = SharedType.PerClass)]
    public required PostgresContainerPool Containers { get; set; }

    [Test]
    public async Task CreateQueue_CreatesMainAndDlq()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        // Act
        var queueId = await CreateQueueAsync(connection, schema, queueName, 5);

        // Assert
        await using var command = new NpgsqlCommand(
            $"""SELECT queue_id, type, max_delivery_count FROM "{schema}".queues WHERE name = $1 ORDER BY type;""",
            connection
        );
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(long QueueId, short Type, int MaxDeliveryCount)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt64(0), reader.GetInt16(1), reader.GetInt32(2)));
        }

        await Assert.That(rows.Count).EqualTo(2);
        await Assert.That(rows[0].Type).EqualTo((short)1);
        await Assert.That(rows[1].Type).EqualTo((short)2);
        await Assert.That(rows[0].MaxDeliveryCount).EqualTo(5);
        await Assert.That(rows[1].MaxDeliveryCount).EqualTo(5);
        await Assert.That(rows[0].QueueId).EqualTo(queueId);
    }

    [Test]
    public async Task SendMessage_InsertsRowsAndDefaultsPriority()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);

        // Act
        var messageId = await SendMessageAsync(connection, schema, queueName, null, null, null);

        // Assert
        await using var priorityCommand = new NpgsqlCommand(
            $"""SELECT priority FROM "{schema}".message_delivery WHERE message_id = $1;""",
            connection
        );
        priorityCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var priority = Convert.ToInt32(
            await priorityCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await using var messageCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".messages WHERE message_id = $1;""",
            connection
        );
        messageCountCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var messageCount = Convert.ToInt64(
            await messageCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(priority).EqualTo(16384);
        await Assert.That(messageCount).EqualTo(1);
    }

    [Test]
    public async Task GetMessages_LocksAndIncrementsDelivery()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var messageId = await SendMessageAsync(connection, schema, queueName, null, null, null);

        // Act
        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        // Assert
        await Assert.That(fetched.MessageId).EqualTo(messageId);
        await Assert.That(fetched.LockId).EqualTo(lockId);
        await Assert.That(fetched.DeliveryCount).EqualTo(1);
    }

    [Test]
    public async Task CompleteMessage_RemovesDeliveryAndMessage()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var messageId = await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        // Act
        var completeSql = $"""SELECT "{schema}".complete_message($1, $2);""";
        var completedId = await ExecuteScalarLongAsync(
            connection,
            completeSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId }
        );

        // Assert
        await using var deliveryCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".message_delivery WHERE message_id = $1;""",
            connection
        );
        deliveryCountCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var deliveryCount = Convert.ToInt64(
            await deliveryCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await using var messageCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".messages WHERE message_id = $1;""",
            connection
        );
        messageCountCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var messageCount = Convert.ToInt64(
            await messageCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(completedId).EqualTo(fetched.DeliveryId);
        await Assert.That(deliveryCount).EqualTo(0);
        await Assert.That(messageCount).EqualTo(0);
    }

    [Test]
    public async Task CompleteMessages_RemovesAllDeliveriesAndMessages()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var messageId1 = await SendMessageAsync(connection, schema, queueName, null, null, null);
        var messageId2 = await SendMessageAsync(connection, schema, queueName, null, null, null);
        var messageId3 = await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched1 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );
        var fetched2 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );
        var fetched3 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        // Act
        var completeSql = $"""SELECT "{schema}".complete_messages($1, $2);""";
        var completedCount = await ExecuteScalarLongAsync(
            connection,
            completeSql,
            CancellationToken.None,
            new NpgsqlParameter<long[]>
            {
                TypedValue = [fetched1.DeliveryId, fetched2.DeliveryId, fetched3.DeliveryId],
            },
            new NpgsqlParameter<Guid[]> { TypedValue = [lockId, lockId, lockId] }
        );

        // Assert
        await using var deliveryCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".message_delivery WHERE message_id = ANY($1);""",
            connection
        );
        deliveryCountCommand.Parameters.Add(
            new NpgsqlParameter<Guid[]> { TypedValue = [messageId1, messageId2, messageId3] }
        );
        var deliveryCount = Convert.ToInt64(
            await deliveryCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await using var messageCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".messages WHERE message_id = ANY($1);""",
            connection
        );
        messageCountCommand.Parameters.Add(
            new NpgsqlParameter<Guid[]> { TypedValue = [messageId1, messageId2, messageId3] }
        );
        var messageCount = Convert.ToInt64(
            await messageCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(completedCount).EqualTo(3);
        await Assert.That(deliveryCount).EqualTo(0);
        await Assert.That(messageCount).EqualTo(0);
    }

    [Test]
    public async Task CompleteMessages_OnlyCompletesMatchingLocks()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId1 = Guid.NewGuid();
        var lockId2 = Guid.NewGuid();
        var fetched1 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId1,
            TimeSpan.FromSeconds(30)
        );
        var fetched2 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId2,
            TimeSpan.FromSeconds(30)
        );

        // Act - try to complete both with wrong lock for message 2
        var completeSql = $"""SELECT "{schema}".complete_messages($1, $2);""";
        var completedCount = await ExecuteScalarLongAsync(
            connection,
            completeSql,
            CancellationToken.None,
            new NpgsqlParameter<long[]> { TypedValue = [fetched1.DeliveryId, fetched2.DeliveryId] },
            new NpgsqlParameter<Guid[]> { TypedValue = [lockId1, Guid.NewGuid()] } // wrong lock for message 2
        );

        // Assert - only message 1 should be completed
        await using var deliveryCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".message_delivery;""",
            connection
        );
        var deliveryCount = Convert.ToInt64(
            await deliveryCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(completedCount).EqualTo(1);
        await Assert.That(deliveryCount).EqualTo(1);
    }

    [Test]
    public async Task AbandonMessage_ClearsLockAndSetsHeaders()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        var headersJson = """{"reason":"test"}""";
        var abandonSql = $"""SELECT "{schema}".abandon_message($1, $2, $3, $4);""";

        // Act
        await ExecuteScalarLongAsync(
            connection,
            abandonSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId },
            new NpgsqlParameter<string>
            {
                TypedValue = headersJson,
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromSeconds(10) }
        );

        // Assert
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT lock_id, message_delivery_headers->>'reason' FROM "{schema}".message_delivery WHERE message_delivery_id = $1;""",
            connection
        );
        checkCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId });

        await using var reader = await checkCommand.ExecuteReaderAsync();
        await reader.ReadAsync();
        Guid? lockValue = reader.IsDBNull(0) ? null : reader.GetGuid(0);
        var reason = reader.GetString(1);

        await Assert.That(lockValue).EqualTo(null);
        await Assert.That(reason).EqualTo("test");
    }

    [Test]
    public async Task AbandonMessages_ClearsLocksAndSetsHeaders()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);
        await SendMessageAsync(connection, schema, queueName, null, null, null);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched1 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );
        var fetched2 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );
        var fetched3 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        var abandonSql = $"""SELECT "{schema}".abandon_messages($1, $2, $3, $4);""";

        // Act
        var abandonedCount = await ExecuteScalarLongAsync(
            connection,
            abandonSql,
            CancellationToken.None,
            new NpgsqlParameter<long[]>
            {
                TypedValue = [fetched1.DeliveryId, fetched2.DeliveryId, fetched3.DeliveryId],
            },
            new NpgsqlParameter<Guid[]> { TypedValue = [lockId, lockId, lockId] },
            new NpgsqlParameter
            {
                Value = new[]
                {
                    """{"reason":"error1"}""",
                    """{"reason":"error2"}""",
                    """{"reason":"error3"}""",
                },
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromSeconds(10) }
        );

        // Assert
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT message_delivery_id, lock_id, message_delivery_headers->>'reason' FROM "{schema}".message_delivery ORDER BY message_delivery_id;""",
            connection
        );

        await using var reader = await checkCommand.ExecuteReaderAsync();
        var results = new List<(long DeliveryId, Guid? LockId, string Reason)>();
        while (await reader.ReadAsync())
        {
            results.Add(
                (
                    reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetGuid(1),
                    reader.GetString(2)
                )
            );
        }

        await Assert.That(abandonedCount).EqualTo(3);
        await Assert.That(results.Count).EqualTo(3);
        await Assert.That(results.All(r => r.LockId == null)).IsTrue();
        await Assert.That(results[0].Reason).EqualTo("error1");
        await Assert.That(results[1].Reason).EqualTo("error2");
        await Assert.That(results[2].Reason).EqualTo("error3");
    }

    [Test]
    public async Task AbandonMessages_OnlyAbandonsMatchingLocks()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId1 = Guid.NewGuid();
        var lockId2 = Guid.NewGuid();
        var fetched1 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId1,
            TimeSpan.FromSeconds(30)
        );
        var fetched2 = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId2,
            TimeSpan.FromSeconds(30)
        );

        var abandonSql = $"""SELECT "{schema}".abandon_messages($1, $2, $3, $4);""";

        // Act - try to abandon both with wrong lock for message 2
        var abandonedCount = await ExecuteScalarLongAsync(
            connection,
            abandonSql,
            CancellationToken.None,
            new NpgsqlParameter<long[]> { TypedValue = [fetched1.DeliveryId, fetched2.DeliveryId] },
            new NpgsqlParameter<Guid[]> { TypedValue = [lockId1, Guid.NewGuid()] }, // wrong lock for message 2
            new NpgsqlParameter
            {
                Value = new[] { """{"reason":"error1"}""", """{"reason":"error2"}""" },
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromSeconds(10) }
        );

        // Assert - only message 1 should be abandoned
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT message_delivery_id, lock_id FROM "{schema}".message_delivery ORDER BY message_delivery_id;""",
            connection
        );

        await using var reader = await checkCommand.ExecuteReaderAsync();
        var results = new List<(long DeliveryId, Guid? LockId)>();
        while (await reader.ReadAsync())
        {
            results.Add((reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetGuid(1)));
        }

        await Assert.That(abandonedCount).EqualTo(1);
        await Assert.That(results.Count).EqualTo(2);
        // Message 1 should have null lock (abandoned)
        await Assert
            .That(results.First(r => r.DeliveryId == fetched1.DeliveryId).LockId)
            .EqualTo(null);
        // Message 2 should still have its lock (not abandoned)
        await Assert
            .That(results.First(r => r.DeliveryId == fetched2.DeliveryId).LockId)
            .EqualTo(lockId2);
    }

    [Test]
    public async Task MoveMessage_MovesToTargetQueue()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var dlqQueueId = await GetQueueIdAsync(connection, schema, queueName, 2);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        var headersJson = """{"moved":"true"}""";
        var moveSql = $"""SELECT "{schema}".move_message($1, $2, $3, $4, $5, $6);""";

        // Act
        await ExecuteScalarLongAsync(
            connection,
            moveSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId },
            new NpgsqlParameter<short> { TypedValue = 2 },
            new NpgsqlParameter<string> { TypedValue = queueName },
            new NpgsqlParameter<string>
            {
                TypedValue = headersJson,
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.Zero }
        );

        // Assert
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT queue_id, lock_id, message_delivery_headers->>'moved' FROM "{schema}".message_delivery WHERE message_delivery_id = $1;""",
            connection
        );
        checkCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId });

        await using var reader = await checkCommand.ExecuteReaderAsync();
        await reader.ReadAsync();
        var queueId = reader.GetInt64(0);
        Guid? lockValue = reader.IsDBNull(1) ? null : reader.GetGuid(1);
        var moved = reader.GetString(2);

        await Assert.That(queueId).EqualTo(dlqQueueId);
        await Assert.That(lockValue).EqualTo(null);
        await Assert.That(moved).EqualTo("true");
    }

    [Test]
    public async Task DeadletterMessage_MovesToDlq()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var dlqQueueId = await GetQueueIdAsync(connection, schema, queueName, 2);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        var headersJson = """{"deadletter":"true"}""";
        var deadletterSql = $"""SELECT "{schema}".deadletter_message($1, $2, $3, $4);""";

        // Act
        await ExecuteScalarLongAsync(
            connection,
            deadletterSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId },
            new NpgsqlParameter<string> { TypedValue = queueName },
            new NpgsqlParameter<string>
            {
                TypedValue = headersJson,
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            }
        );

        // Assert
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT queue_id FROM "{schema}".message_delivery WHERE message_delivery_id = $1;""",
            connection
        );
        checkCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId });
        var queueId = Convert.ToInt64(
            await checkCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(queueId).EqualTo(dlqQueueId);
    }

    [Test]
    public async Task RenewMessageLock_UpdatesVisibleAt()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(5)
        );

        var renewSql = $"""SELECT "{schema}".renew_message_lock($1, $2, $3);""";

        // Act
        await ExecuteScalarLongAsync(
            connection,
            renewSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromSeconds(30) }
        );

        // Assert
        await using var checkCommand = new NpgsqlCommand(
            $"""SELECT visible_at FROM "{schema}".message_delivery WHERE message_delivery_id = $1;""",
            connection
        );
        checkCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId });
        await using var reader = await checkCommand.ExecuteReaderAsync();
        await reader.ReadAsync();
        var renewedVisibleAt = reader.GetFieldValue<DateTimeOffset>(0);

        await Assert.That(renewedVisibleAt).IsNotEqualTo(fetched.VisibleAt);
    }

    [Test]
    public async Task DeleteScheduledMessage_RemovesMessageAndDelivery()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        var schedulingToken = Guid.NewGuid();
        var messageId = await SendMessageAsync(
            connection,
            schema,
            queueName,
            null,
            schedulingToken,
            TimeSpan.Zero
        );

        var deleteSql = $"""SELECT "{schema}".cancel_scheduled_message($1);""";

        // Act
        await ExecuteScalarAsync(
            connection,
            deleteSql,
            CancellationToken.None,
            new NpgsqlParameter<Guid> { TypedValue = schedulingToken }
        );

        // Assert
        await using var messageCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".messages WHERE message_id = $1;""",
            connection
        );
        messageCountCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var messageCount = Convert.ToInt64(
            await messageCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await using var deliveryCountCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".message_delivery WHERE message_id = $1;""",
            connection
        );
        deliveryCountCommand.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = messageId });
        var deliveryCount = Convert.ToInt64(
            await deliveryCountCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        await Assert.That(messageCount).EqualTo(0);
        await Assert.That(deliveryCount).EqualTo(0);
    }

    [Test]
    public async Task PeekDeadletters_ReturnsMessages()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string queueName = "orders";
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        await CreateQueueAsync(connection, schema, queueName, 10);
        await SendMessageAsync(connection, schema, queueName, null, null, null);

        var lockId = Guid.NewGuid();
        var fetched = await GetMessageAsync(
            connection,
            schema,
            queueName,
            lockId,
            TimeSpan.FromSeconds(30)
        );

        var deadletterSql = $"""SELECT "{schema}".deadletter_message($1, $2, $3, $4);""";
        await ExecuteScalarLongAsync(
            connection,
            deadletterSql,
            CancellationToken.None,
            new NpgsqlParameter<long> { TypedValue = fetched.DeliveryId },
            new NpgsqlParameter<Guid> { TypedValue = lockId },
            new NpgsqlParameter<string> { TypedValue = queueName },
            new NpgsqlParameter<string>
            {
                TypedValue = """{"deadletter":"true"}""",
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            }
        );

        // Act
        var peekSql = $"""SELECT * FROM "{schema}".peek_deadletters($1, $2);""";
        await using var peekCommand = new NpgsqlCommand(peekSql, connection);
        peekCommand.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        peekCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 10 });
        await using var reader = await peekCommand.ExecuteReaderAsync();

        await reader.ReadAsync();
        var returnedQueueName = reader.GetString(1);
        var deliveryId = reader.GetInt64(2);

        // Assert
        await Assert.That(returnedQueueName).EqualTo(queueName);
        await Assert.That(deliveryId).EqualTo(fetched.DeliveryId);
    }

    [Test]
    public async Task AcquireLock_RespectsExistingAndExpiredLocks()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string lockId = "test-lock";
        var ownerToken = Guid.NewGuid();
        var secondOwnerToken = Guid.NewGuid();
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        var acquireSql = $"""SELECT "{schema}".acquire_lock($1, $2, $3);""";

        // Act
        var firstAcquire = await ExecuteScalarBoolAsync(
            connection,
            acquireSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromHours(1) },
            new NpgsqlParameter<Guid> { TypedValue = ownerToken }
        );
        var secondAcquire = await ExecuteScalarBoolAsync(
            connection,
            acquireSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromHours(1) },
            new NpgsqlParameter<Guid> { TypedValue = secondOwnerToken }
        );

        await using (
            var expireCommand = new NpgsqlCommand(
                $"""UPDATE "{schema}".distributed_locks SET expires_at = (NOW() AT TIME ZONE 'utc') - INTERVAL '1 second' WHERE lock_id = $1;""",
                connection
            )
        )
        {
            expireCommand.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });
            await expireCommand.ExecuteNonQueryAsync();
        }

        var thirdAcquire = await ExecuteScalarBoolAsync(
            connection,
            acquireSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromSeconds(10) },
            new NpgsqlParameter<Guid> { TypedValue = secondOwnerToken }
        );

        // Assert
        await Assert.That(firstAcquire).EqualTo(true);
        await Assert.That(secondAcquire).EqualTo(false);
        await Assert.That(thirdAcquire).EqualTo(true);
    }

    [Test]
    public async Task ReleaseLock_RemovesOwnedLock()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string lockId = "release-lock";
        var ownerToken = Guid.NewGuid();
        var otherToken = Guid.NewGuid();
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        var acquireSql = $"""SELECT "{schema}".acquire_lock($1, $2, $3);""";
        var releaseSql = $"""SELECT "{schema}".release_lock($1, $2);""";

        var acquired = await ExecuteScalarBoolAsync(
            connection,
            acquireSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromMinutes(5) },
            new NpgsqlParameter<Guid> { TypedValue = ownerToken }
        );

        // Act
        var releaseWrongOwner = await ExecuteScalarBoolAsync(
            connection,
            releaseSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<Guid> { TypedValue = otherToken }
        );
        var releaseRightOwner = await ExecuteScalarBoolAsync(
            connection,
            releaseSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<Guid> { TypedValue = ownerToken }
        );

        await using var countCommand = new NpgsqlCommand(
            $"""SELECT COUNT(*) FROM "{schema}".distributed_locks WHERE lock_id = $1;""",
            connection
        );
        countCommand.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });
        var remaining = Convert.ToInt64(
            await countCommand.ExecuteScalarAsync(),
            CultureInfo.InvariantCulture
        );

        // Assert
        await Assert.That(acquired).EqualTo(true);
        await Assert.That(releaseWrongOwner).EqualTo(false);
        await Assert.That(releaseRightOwner).EqualTo(true);
        await Assert.That(remaining).EqualTo(0);
    }

    [Test]
    public async Task RenewLock_ExtendsActiveLock()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schema = NewSchema();
        const string lockId = "renew-lock";
        var ownerToken = Guid.NewGuid();
        var otherToken = Guid.NewGuid();
        await using var dataSource = await CreateMigratedDataSourceAsync(
            container,
            schema,
            CancellationToken.None
        );
        await using var connection = await dataSource.OpenConnectionAsync();

        var acquireSql = $"""SELECT "{schema}".acquire_lock($1, $2, $3);""";
        var renewSql = $"""SELECT "{schema}".renew_lock($1, $2, $3);""";

        var acquired = await ExecuteScalarBoolAsync(
            connection,
            acquireSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromHours(1) },
            new NpgsqlParameter<Guid> { TypedValue = ownerToken }
        );

        var initialState = await GetLockStateAsync(connection, schema, lockId);

        // Act
        var renewWrongOwner = await ExecuteScalarBoolAsync(
            connection,
            renewSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<Guid> { TypedValue = otherToken },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromHours(1) }
        );
        var renewed = await ExecuteScalarBoolAsync(
            connection,
            renewSql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = lockId },
            new NpgsqlParameter<Guid> { TypedValue = ownerToken },
            new NpgsqlParameter<TimeSpan> { TypedValue = TimeSpan.FromHours(1) }
        );

        var updatedState = await GetLockStateAsync(connection, schema, lockId);

        // Assert
        await Assert.That(acquired).EqualTo(true);
        await Assert.That(renewWrongOwner).EqualTo(false);
        await Assert.That(renewed).EqualTo(true);
        await Assert.That(updatedState.ExpiresAt).IsNotEqualTo(initialState.ExpiresAt);
        await Assert.That(updatedState.ExtendedTimes).EqualTo(initialState.ExtendedTimes + 1);
    }

    private static string NewSchema() => $"bussig_test_{Guid.NewGuid():N}";

    private static async Task<NpgsqlDataSource> CreateMigratedDataSourceAsync(
        PostgreSqlContainer container,
        string schema,
        CancellationToken cancellationToken
    )
    {
        var dataSource = NpgsqlDataSource.Create(container.GetConnectionString());
        var options = new PostgresSettings
        {
            ConnectionString = container.GetConnectionString(),
            Schema = schema,
        };
        var migrator = new Bussig.PostgresMigrator(
            dataSource,
            Options.Create<PostgresSettings>(options),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        await migrator.CreateSchema(cancellationToken);
        await migrator.CreateInfrastructure(cancellationToken);

        return dataSource;
    }

    private static async Task<long> CreateQueueAsync(
        NpgsqlConnection connection,
        string schema,
        string name,
        int? maxDeliveryCount
    )
    {
        var sql = string.Format(CultureInfo.InvariantCulture, PsqlStatements.CreateQueue, schema);
        return await ExecuteScalarLongAsync(
            connection,
            sql,
            CancellationToken.None,
            new NpgsqlParameter<string> { TypedValue = name },
            new NpgsqlParameter<int?> { TypedValue = maxDeliveryCount }
        );
    }

    private static async Task<Guid> SendMessageAsync(
        NpgsqlConnection connection,
        string schema,
        string queueName,
        short? priority,
        Guid? schedulingToken,
        TimeSpan? delay
    )
    {
        var messageId = Guid.NewGuid();
        var sql = string.Format(CultureInfo.InvariantCulture, PsqlStatements.SendMessage, schema);
        var parameters = new NpgsqlParameter[]
        {
            new NpgsqlParameter<string> { TypedValue = queueName },
            new NpgsqlParameter<Guid> { TypedValue = messageId },
            new NpgsqlParameter<short?> { TypedValue = priority },
            new NpgsqlParameter<byte[]>
            {
                TypedValue = JsonSerializer.SerializeToUtf8Bytes(new { ok = true }),
            },
            new NpgsqlParameter<TimeSpan> { TypedValue = delay ?? TimeSpan.Zero },
            new NpgsqlParameter<string>
            {
                TypedValue = """{"message-types":["test"]}""",
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<int> { TypedValue = 0 },
            new NpgsqlParameter<DateTimeOffset?> { TypedValue = null },
            new NpgsqlParameter<Guid?> { TypedValue = schedulingToken },
        };

        await ExecuteScalarAsync(connection, sql, CancellationToken.None, parameters);
        return messageId;
    }

    private static async Task<FetchedMessage> GetMessageAsync(
        NpgsqlConnection connection,
        string schema,
        string queueName,
        Guid lockId,
        TimeSpan lockDuration
    )
    {
        var sql = string.Format(CultureInfo.InvariantCulture, PsqlStatements.GetMessages, schema);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });
        command.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = lockDuration });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            throw new InvalidOperationException("No messages returned.");
        }

        return new FetchedMessage(
            reader.GetGuid(0),
            reader.GetInt64(1),
            reader.GetInt16(2),
            reader.GetInt64(3),
            reader.GetFieldValue<DateTimeOffset>(4),
            reader.GetGuid(10),
            reader.GetInt32(13)
        );
    }

    private static async Task<long> GetQueueIdAsync(
        NpgsqlConnection connection,
        string schema,
        string name,
        short type
    )
    {
        await using var command = new NpgsqlCommand(
            $"""SELECT queue_id FROM "{schema}".queues WHERE name = $1 AND type = $2;""",
            connection
        );
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
        command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = type });
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private static async Task<object?> ExecuteScalarAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken cancellationToken,
        params NpgsqlParameter[] parameters
    )
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private static async Task<long> ExecuteScalarLongAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken cancellationToken,
        params NpgsqlParameter[] parameters
    )
    {
        var result = await ExecuteScalarAsync(connection, sql, cancellationToken, parameters);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private static async Task<bool> ExecuteScalarBoolAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken cancellationToken,
        params NpgsqlParameter[] parameters
    )
    {
        var result = await ExecuteScalarAsync(connection, sql, cancellationToken, parameters);
        return Convert.ToBoolean(result, CultureInfo.InvariantCulture);
    }

    private static async Task<(DateTimeOffset ExpiresAt, int ExtendedTimes)> GetLockStateAsync(
        NpgsqlConnection connection,
        string schema,
        string lockId
    )
    {
        await using var command = new NpgsqlCommand(
            $"""SELECT expires_at, extended_times FROM "{schema}".distributed_locks WHERE lock_id = $1;""",
            connection
        );
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });

        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();

        return (reader.GetFieldValue<DateTimeOffset>(0), reader.GetInt32(1));
    }

    private sealed record FetchedMessage(
        Guid MessageId,
        long DeliveryId,
        short Priority,
        long QueueId,
        DateTimeOffset VisibleAt,
        Guid LockId,
        int DeliveryCount
    );
}
