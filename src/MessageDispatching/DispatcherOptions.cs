namespace MessageDispatching;

public sealed class DispatcherOptions
{
    private const int MaximumObservationSamples = 1_000_000;
    private static readonly TimeSpan MaximumTimerInterval =
        TimeSpan.FromMilliseconds(uint.MaxValue - 1d);

    public int Parallelism { get; init; } = Math.Max(1, Environment.ProcessorCount);

    public int MaxParallelism { get; init; }

    public int KeyBatchSize { get; init; } = 32;

    public Action<DispatcherScaleChange>? ScaleObserver { get; init; }

    public TimeSpan ScaleInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    public TimeSpan ScaleObservationWindow { get; init; } = TimeSpan.FromSeconds(2);

    public double ScaleUpSaturationThreshold { get; init; } = 0.80;

    public double ScaleDownUtilizationThreshold { get; init; } = 0.70;

    public TimeSpan ScaleUpCooldown { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan ScaleDownCooldown { get; init; } = TimeSpan.FromSeconds(2);

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

        if (ScaleInterval <= TimeSpan.Zero || ScaleInterval > MaximumTimerInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleInterval),
                $"ScaleInterval must be greater than zero and no greater than {MaximumTimerInterval}.");
        }

        // Compare by division rather than multiplying ScaleInterval, because doubling a valid
        // TimeSpan near TimeSpan.MaxValue can overflow before the comparison is made.
        if (ScaleObservationWindow <= TimeSpan.Zero ||
            ScaleObservationWindow > MaximumTimerInterval ||
            ScaleInterval.Ticks > ScaleObservationWindow.Ticks / 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleObservationWindow),
                $"ScaleObservationWindow must be at least twice ScaleInterval and no greater than {MaximumTimerInterval}.");
        }

        var sampleCapacity = ScaleObservationWindow.Ticks / ScaleInterval.Ticks;
        if (ScaleObservationWindow.Ticks % ScaleInterval.Ticks != 0)
        {
            sampleCapacity++;
        }

        if (sampleCapacity > MaximumObservationSamples)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleObservationWindow),
                $"ScaleObservationWindow cannot contain more than {MaximumObservationSamples} ScaleInterval samples.");
        }

        if (!double.IsFinite(ScaleUpSaturationThreshold) ||
            ScaleUpSaturationThreshold <= 0 ||
            ScaleUpSaturationThreshold > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpSaturationThreshold),
                "ScaleUpSaturationThreshold must be finite and greater than zero and no greater than one.");
        }

        if (!double.IsFinite(ScaleDownUtilizationThreshold) ||
            ScaleDownUtilizationThreshold < 0 ||
            ScaleDownUtilizationThreshold >= 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownUtilizationThreshold),
                "ScaleDownUtilizationThreshold must be finite, zero or greater, and less than one.");
        }

        if (ScaleDownUtilizationThreshold >= ScaleUpSaturationThreshold)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownUtilizationThreshold),
                "ScaleDownUtilizationThreshold must be less than ScaleUpSaturationThreshold.");
        }

        if (ScaleUpCooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpCooldown),
                "ScaleUpCooldown must be zero or greater.");
        }

        if (ScaleDownCooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownCooldown),
                "ScaleDownCooldown must be zero or greater.");
        }
    }
}
