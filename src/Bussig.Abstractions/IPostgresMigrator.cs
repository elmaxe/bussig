namespace Bussig.Abstractions;

public interface IPostgresMigrator
{
    Task CreateDatabase(CancellationToken cancellationToken);
    Task CreateSchema(CancellationToken cancellationToken);
    Task CreateInfrastructure(CancellationToken cancellationToken);
    Task DeleteDatabase(CancellationToken cancellationToken);
}
