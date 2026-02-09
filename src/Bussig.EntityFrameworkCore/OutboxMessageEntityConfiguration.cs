using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Bussig.EntityFrameworkCore;

internal sealed class OutboxMessageEntityConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.HasKey(e => e.Id);
        builder.Property(e => e.MessageId).IsRequired();
        builder.Property(e => e.QueueName).IsRequired();
        builder.Property(e => e.Body).IsRequired();
        builder.Property(e => e.MessageVersion).HasDefaultValue(1).IsRequired();
        builder.Property(e => e.CreatedAt).IsRequired();
        builder.HasIndex(e => e.Id);
        builder.HasIndex(e => e.SchedulingTokenId);

        builder.HasIndex(x => x.PublishedAt);
    }
}
