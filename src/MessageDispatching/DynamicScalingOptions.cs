namespace MessageDispatching;

public sealed class DynamicScalingOptions
{
    private static readonly TimeSpan MaximumTimerInterval =
        TimeSpan.FromMilliseconds(uint.MaxValue - 1d);

    public TimeSpan SampleInterval { get; init; } = TimeSpan.FromMilliseconds(500);

    public double MinimumUsefulThroughputGain { get; init; } = 0.02;

    public double ThroughputSmoothingFactor { get; init; } = 0.25;

    public int ProbeWarmupSamples { get; init; } = 1;

    public int ThroughputMeasurementSamples { get; init; } = 3;

    // Includes convergence and warmup; restarting a measurement window does not extend the deadline.
    public TimeSpan ProbeTimeout { get; init; } = TimeSpan.FromSeconds(10);

    public TimeSpan ScaleUpCooldown { get; init; } = TimeSpan.FromSeconds(2);

    public TimeSpan ScaleDownIdleDuration { get; init; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        if (SampleInterval <= TimeSpan.Zero || SampleInterval > MaximumTimerInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SampleInterval),
                $"SampleInterval must be greater than zero and no greater than {MaximumTimerInterval}.");
        }

        if (!double.IsFinite(MinimumUsefulThroughputGain) || MinimumUsefulThroughputGain < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MinimumUsefulThroughputGain),
                "MinimumUsefulThroughputGain must be finite and zero or greater.");
        }

        if (!double.IsFinite(ThroughputSmoothingFactor) ||
            ThroughputSmoothingFactor <= 0 ||
            ThroughputSmoothingFactor > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ThroughputSmoothingFactor),
                "ThroughputSmoothingFactor must be finite, greater than zero, and no greater than one.");
        }

        if (ProbeWarmupSamples < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ProbeWarmupSamples),
                "ProbeWarmupSamples must be zero or greater.");
        }

        if (ThroughputMeasurementSamples <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ThroughputMeasurementSamples),
                "ThroughputMeasurementSamples must be greater than zero.");
        }

        if (ProbeTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ProbeTimeout),
                "ProbeTimeout must be greater than zero.");
        }

        if (ScaleUpCooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleUpCooldown),
                "ScaleUpCooldown must be zero or greater.");
        }

        if (ScaleDownIdleDuration < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownIdleDuration),
                "ScaleDownIdleDuration must be zero or greater.");
        }
    }
}
