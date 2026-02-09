using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Bussig.EntityFrameworkCore;

public static class ModelBuilderExtensions
{
    public static ModelBuilder AddOutboxMessageEntity(
        this ModelBuilder modelBuilder,
        Action<EntityTypeBuilder<OutboxMessage>>? configure = null
    )
    {
        modelBuilder.ApplyConfiguration(new OutboxMessageEntityConfiguration());

        if (configure is not null)
        {
            modelBuilder.Entity<OutboxMessage>(configure);
        }

        return modelBuilder;
    }
}
