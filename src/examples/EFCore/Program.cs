using System.ComponentModel.DataAnnotations;
using Bussig;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Configuration;
using Bussig.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

// TODO: See if we can remove the explicit OutboxTransactionContext

builder
    .Services.AddOptions<PostgresSettings>()
    .Configure(options =>
    {
        options.ConnectionString =
            "Host=localhost;Port=5432;User Id=postgres;Password=password;Search Path=test;Database=a_cool_db;";
    });
builder.Services.AddMigrationHostedService();
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(
        "Host=localhost;Port=5432;User Id=postgres;Password=password;Database=a_cool_db;"
    )
);
builder.Services.AddOptions<OutboxOptions>();
builder
    .Services.AddBussig(
        (configurator, services) =>
        {
            configurator.AddProcessor<ExampleProcessor>(options =>
            {
                options.Lock.Duration = TimeSpan.FromMinutes(2);
            });
        }
    )
    .AddBussigHostedService()
    .AddBussigOutbox<AppDbContext>()
    .UseNpgsql();

builder.Services.AddHostedService<MigrateDb>();
builder.Services.AddHostedService<SendTestMessage>();
var host = builder.Build();

await host.RunAsync();

sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<SomeEntity> SomeEntities { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddOutboxMessageEntity();
    }
}

sealed class SomeEntity
{
    [Key]
    public Guid Id { get; set; }
    public string Name { get; set; } = null!;
    public DateTimeOffset CreatedAt { get; set; }
}

[MessageMapping("example-message")]
sealed record ExampleMessage : IMessage
{
    public required string Name { get; init; }
}

sealed class ExampleProcessor : IProcessor<ExampleMessage>
{
    public Task ProcessAsync(
        ProcessorContext<ExampleMessage> context,
        CancellationToken cancellationToken = default
    )
    {
        return Task.CompletedTask;
    }
}

sealed class SendTestMessage(
    IBus bus,
    IServiceScopeFactory scopeFactory,
    OutboxTransactionContext outboxTransactionContext
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        await using var scope = scopeFactory.CreateAsyncScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        using var _ = outboxTransactionContext.Use(dbContext);

        var entity = new SomeEntity { Name = "John Smith" };
        dbContext.SomeEntities.Add(entity);

        await bus.SendAsync(new ExampleMessage { Name = entity.Name }, stoppingToken);

        await dbContext.SaveChangesAsync(stoppingToken);
    }
}

sealed class MigrateDb(IServiceScopeFactory scopeFactory) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        await scope
            .ServiceProvider.GetRequiredService<AppDbContext>()
            .Database.MigrateAsync(stoppingToken);
    }
}
