namespace MessageDispatching;

public sealed class DispatcherOptions
{
    public int Parallelism { get; init; } = Math.Max(1, Environment.ProcessorCount);

    public int MaxParallelism { get; init; }

    public int KeyBatchSize { get; init; } = 32;

    public Action<DispatcherScaleChange>? ScaleObserver { get; init; }

    public DynamicScalingOptions DynamicScaling { get; init; } = new();

    internal int EffectiveMaxParallelism => MaxParallelism == 0 ? Parallelism : MaxParallelism;

    internal bool IsDynamicScalingEnabled => MaxParallelism > Parallelism;

    public void Validate()
    {
        if (Parallelism <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Parallelism), "Parallelism must be greater than zero.");
        }

        if (KeyBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(KeyBatchSize), "KeyBatchSize must be greater than zero.");
        }

        if (MaxParallelism < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxParallelism),
                "MaxParallelism must be zero or greater.");
        }

        if (MaxParallelism > 0 && MaxParallelism < Parallelism)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxParallelism),
                "MaxParallelism must be zero or greater than or equal to Parallelism.");
        }

        ArgumentNullException.ThrowIfNull(DynamicScaling);
        DynamicScaling.Validate();
    }
}
