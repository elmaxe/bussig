using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Bussig.Outbox.Npgsql.EntityFrameworkCore;

internal sealed class OutboxMessageEntityConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.HasKey(e => e.Id);

        builder.Property(e => e.Id).UseIdentityAlwaysColumn();

        builder.Property(e => e.MessageId).IsRequired();

        builder.Property(e => e.QueueName).HasColumnType("TEXT").IsRequired();

        builder.Property(e => e.Body).HasColumnType("BYTEA").IsRequired();

        builder.Property(e => e.HeadersJson).HasColumnType("TEXT");

        builder.Property(e => e.Priority).HasColumnType("SMALLINT");

        builder.Property(e => e.Delay).HasColumnType("INTERVAL");

        builder
            .Property(e => e.MessageVersion)
            .HasColumnType("INTEGER")
            .HasDefaultValue(1)
            .IsRequired();

        builder
            .Property(e => e.CreatedAt)
            .HasDefaultValueSql("NOW() AT TIME ZONE 'utc'")
            .IsRequired();

        builder.HasIndex(e => e.Id);

        builder.HasIndex(e => e.SchedulingTokenId);
    }
}
