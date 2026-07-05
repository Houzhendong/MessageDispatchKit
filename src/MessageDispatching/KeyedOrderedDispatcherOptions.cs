namespace MessageDispatching;

public sealed class KeyedOrderedDispatcherOptions
{
    public int Parallelism { get; init; } = Math.Max(1, Environment.ProcessorCount);

    public int BatchSize { get; init; } = 32;

    public void Validate()
    {
        if (Parallelism <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Parallelism), "Parallelism must be greater than zero.");
        }

        if (BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "BatchSize must be greater than zero.");
        }
    }
}
