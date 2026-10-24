namespace MessageDispatching;

public sealed class DynamicScalingOptions
{
    private static readonly TimeSpan MaximumTimerInterval =
        TimeSpan.FromMilliseconds(uint.MaxValue - 1d);

    public TimeSpan SampleInterval { get; init; } = TimeSpan.FromMilliseconds(500);

    public double CapacityGainTolerance { get; init; } = 0.02;

    public double ThroughputSmoothingFactor { get; init; } = 0.25;

    public TimeSpan ScaleDownIdleDuration { get; init; } = TimeSpan.FromSeconds(5);

    // Both dispatchers use the same load-matching controller.
    public double TargetUtilization { get; init; } = 0.8;

    public TimeSpan UtilizationWindow { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan TargetQueueWait { get; init; } = TimeSpan.FromMilliseconds(50);

    public TimeSpan ScaleUpInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    // Zero uses Environment.ProcessorCount. Set explicitly for blocking handlers or a CPU budget.
    public int ResourceParallelismLimit { get; init; }

    public double CpuUtilizationLimit { get; init; } = 0.9;

    public TimeSpan CapacityMeasurementWindow { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan CapacityRetryInterval { get; init; } = TimeSpan.FromSeconds(10);

    internal void Validate()
    {
        if (SampleInterval < TimeSpan.FromMilliseconds(1) || SampleInterval > MaximumTimerInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SampleInterval),
                $"SampleInterval must be at least 1ms and no greater than {MaximumTimerInterval}.");
        }

        if (!double.IsFinite(CapacityGainTolerance) || CapacityGainTolerance < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(CapacityGainTolerance),
                "CapacityGainTolerance must be finite and zero or greater.");
        }

        if (!double.IsFinite(ThroughputSmoothingFactor) ||
            ThroughputSmoothingFactor <= 0 ||
            ThroughputSmoothingFactor > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ThroughputSmoothingFactor),
                "ThroughputSmoothingFactor must be finite, greater than zero, and no greater than one.");
        }

        if (ScaleDownIdleDuration < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScaleDownIdleDuration),
                "ScaleDownIdleDuration must be zero or greater.");
        }

        ValidateFraction(TargetUtilization, nameof(TargetUtilization));
        ValidateFraction(CpuUtilizationLimit, nameof(CpuUtilizationLimit));
        ValidatePositiveDuration(UtilizationWindow, nameof(UtilizationWindow));
        ValidatePositiveDuration(TargetQueueWait, nameof(TargetQueueWait));
        ValidatePositiveDuration(CapacityMeasurementWindow, nameof(CapacityMeasurementWindow));
        ValidatePositiveDuration(CapacityRetryInterval, nameof(CapacityRetryInterval));
        if (ScaleUpInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ScaleUpInterval));
        }

        if (ResourceParallelismLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(ResourceParallelismLimit));
        }
    }

    private static void ValidateFraction(double value, string name)
    {
        if (!double.IsFinite(value) || value <= 0 || value > 1)
        {
            throw new ArgumentOutOfRangeException(name, "Value must be finite, greater than zero, and at most one.");
        }
    }

    private static void ValidatePositiveDuration(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(name, "Duration must be greater than zero.");
        }
    }
}
