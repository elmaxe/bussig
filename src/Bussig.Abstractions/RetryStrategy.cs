namespace Bussig.Abstractions;

public enum RetryStrategy
{
    Immediate = 0,
    Default = Immediate,
    Fixed = 1,
    Exponential = 2,
    Custom = 3,
}
