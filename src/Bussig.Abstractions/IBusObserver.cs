namespace Bussig.Abstractions;

public interface IBusObserver
{
    Task PreStartAsync();
    Task PostStartAsync();
    Task PreStopAsync();
    Task PostStopAsync();
}
