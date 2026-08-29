namespace MessageDispatching;

public sealed class DispatcherOptions
{
    public int Parallelism { get; init; } = Math.Max(1, Environment.ProcessorCount);

    public int MaxParallelism { get; init; }

    public int KeyBatchSize { get; init; } = 32;

    public Action<DispatcherScaleChange>? ScaleObserver { get; init; }

    public TimeSpan ScaleInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    public TimeSpan ScaleUpCooldown { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan ScaleDownIdleDuration { get; init; } = TimeSpan.FromSeconds(30);

    public int ScaleUpQueuedWorkItemsThreshold { get; init; }

    public int ScaleUpConsecutiveSamples { get; init; } = 2;

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

        if (ScaleInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleInterval),
                "ScaleInterval must be greater than zero.");
        }

        if (ScaleUpCooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpCooldown),
                "ScaleUpCooldown must be zero or greater.");
        }

        if (ScaleDownIdleDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownIdleDuration),
                "ScaleDownIdleDuration must be greater than zero.");
        }

        if (ScaleUpQueuedWorkItemsThreshold < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpQueuedWorkItemsThreshold),
                "ScaleUpQueuedWorkItemsThreshold must be zero or greater.");
        }

        if (ScaleUpConsecutiveSamples <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpConsecutiveSamples),
                "ScaleUpConsecutiveSamples must be greater than zero.");
        }
    }
}
