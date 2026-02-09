using Bussig;
using Bussig.Configuration;
using Bussig.Outbox.Npgsql;

var builder = Host.CreateApplicationBuilder(args);

builder
    .Services.AddOptions<PostgresSettings>()
    .Configure(options =>
    {
        options.ConnectionString =
            "Host=localhost;Port=5432;User Id=postgres;Password=password;Search Path=test;Database=a_cool_db;";
    });
builder.Services.AddMigrationHostedService();
builder
    .Services.AddBussig(
        (configurator, services) =>
        {
            configurator.AddProcessor<TestMessageProcessor>(options =>
            {
                options.Lock.Duration = TimeSpan.FromMinutes(2);
                options.Polling.SingletonProcessing.EnableSingletonProcessing = true;
            });
            // configurator.UseAttachments();
            // services.UseInMemoryAttachments();
            // services.UseAzureBlobStorageAttachments((options, sp) =>
            // {
            //     // options.ConnectionString = "";
            //     options.TokenCredential = sp.GetRequiredService<TokenCredential>();
            //     options.StorageAccountName = "bussig";
            // });

            // configurator.AddMessage<TestMessage>(options => options.MaxDeliveryCount = 10);
        }
    )
    .AddBussigHostedService()
    .AddBussigNpgsqlOutbox(options =>options. );
var host = builder.Build();
await host.RunAsync();
