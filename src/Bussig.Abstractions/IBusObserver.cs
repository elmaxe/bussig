namespace Bussig.Abstractions;

// TODO: Call these functions
public interface IBusObserver
{
    Task PreStartAsync();
    Task PostStartAsync();
    Task PreStopAsync();
    Task PostStopAsync();
}
