namespace Bussig.Abstractions.Host;

public class MigrationOptions
{
    public bool CreateDatabase { get; set; }
    public bool CreateSchema { get; set; }
    public bool CreateInfrastructure { get; set; }

    /// <summary>
    /// Do not use in prod
    /// </summary>
    public bool DeleteDatabase { get; set; }
}
