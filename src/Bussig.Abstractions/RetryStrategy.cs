namespace Bussig.Abstractions;

public enum RetryStrategy
{
    Default = Immediate,
    Immediate = 0,
    Fixed = 1,
    Exponential = 2,
    Custom = 3,
}
