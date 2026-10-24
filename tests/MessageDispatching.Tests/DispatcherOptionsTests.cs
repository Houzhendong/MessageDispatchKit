using Xunit;

namespace MessageDispatching.Tests;

public sealed class DispatcherOptionsTests
{
    [Fact]
    public void DefaultsAreValidAndDynamicScalingDefaultsAreNested()
    {
        var options = new DispatcherOptions();

        Assert.True(options.Parallelism >= 1);
        Assert.Equal(0, options.MaxParallelism);
        Assert.Equal(32, options.KeyBatchSize);
        Assert.Null(options.ScaleObserver);
        Assert.NotNull(options.DynamicScaling);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.DynamicScaling.SampleInterval);
        Assert.Equal(0.02, options.DynamicScaling.CapacityGainTolerance);
        Assert.Equal(0.25, options.DynamicScaling.ThroughputSmoothingFactor);
        Assert.Equal(0.8, options.DynamicScaling.TargetUtilization);
        Assert.Equal(TimeSpan.FromMilliseconds(200), options.DynamicScaling.ScaleUpInterval);
        Assert.Equal(0, options.DynamicScaling.ResourceParallelismLimit);
        Assert.Equal(TimeSpan.FromSeconds(5), options.DynamicScaling.ScaleDownIdleDuration);
        options.Validate();
    }

    [Theory]
    [InlineData("ScaleInterval")]
    [InlineData("ScaleObservationWindow")]
    [InlineData("ScaleUpSaturationThreshold")]
    [InlineData("ScaleDownUtilizationThreshold")]
    [InlineData("ScaleUpCooldown")]
    [InlineData("ScaleDownCooldown")]
    public void RemovedFlatDynamicScalingMembersAreNotExposed(string memberName)
    {
        Assert.Null(typeof(DispatcherOptions).GetProperty(memberName));
    }

    [Theory]
    [InlineData("ProbeWarmupSamples")]
    [InlineData("ThroughputMeasurementSamples")]
    [InlineData("ProbeTimeout")]
    [InlineData("ScaleUpCooldown")]
    [InlineData("MinimumUsefulThroughputGain")]
    public void LegacyProbeOptionsAreRemoved(string memberName)
    {
        Assert.Null(typeof(DynamicScalingOptions).GetProperty(memberName));
    }

    [Theory]
    [InlineData("ProbeAcceptCount")]
    [InlineData("ProbeRejectCount")]
    [InlineData("LastProbeGain")]
    public void LegacyProbeStatsAreRemoved(string memberName)
    {
        Assert.Null(typeof(KeyedDispatcherStats).GetProperty(memberName));
        Assert.Null(typeof(DispatcherStats).GetProperty(memberName));
        Assert.Null(typeof(DispatcherScalingStats).GetProperty(memberName));
    }

    [Fact]
    public void DynamicScalingMustNotBeNull()
    {
        var options = new DispatcherOptions
        {
            DynamicScaling = null!
        };

        var exception = Assert.Throws<ArgumentNullException>(options.Validate);
        Assert.Equal(nameof(DispatcherOptions.DynamicScaling), exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void SampleIntervalMustBePositive(int milliseconds)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                SampleInterval = TimeSpan.FromMilliseconds(milliseconds)
            });

        AssertParameter(nameof(DynamicScalingOptions.SampleInterval), options);
    }

    [Fact]
    public void SampleIntervalMustFitPeriodicTimer()
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                SampleInterval = TimeSpan.FromMilliseconds(uint.MaxValue)
            });

        AssertParameter(nameof(DynamicScalingOptions.SampleInterval), options);
    }

    [Fact]
    public void SubMillisecondSampleIntervalIsRejectedBeforeStartingWorkers()
    {
        AssertParameter(nameof(DynamicScalingOptions.SampleInterval),
            OptionsWithScaling(new DynamicScalingOptions { SampleInterval = TimeSpan.FromTicks(1) }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void LoadMatchingFractionsAreValidated(double value)
    {
        AssertParameter(nameof(DynamicScalingOptions.TargetUtilization),
            OptionsWithScaling(new DynamicScalingOptions { TargetUtilization = value }));
        AssertParameter(nameof(DynamicScalingOptions.CpuUtilizationLimit),
            OptionsWithScaling(new DynamicScalingOptions { CpuUtilizationLimit = value }));
    }

    [Fact]
    public void LoadMatchingBudgetsAndWindowsAreValidated()
    {
        AssertParameter(nameof(DynamicScalingOptions.ResourceParallelismLimit),
            OptionsWithScaling(new DynamicScalingOptions { ResourceParallelismLimit = -1 }));
        AssertParameter(nameof(DynamicScalingOptions.UtilizationWindow),
            OptionsWithScaling(new DynamicScalingOptions { UtilizationWindow = TimeSpan.Zero }));
        AssertParameter(nameof(DynamicScalingOptions.TargetQueueWait),
            OptionsWithScaling(new DynamicScalingOptions { TargetQueueWait = TimeSpan.Zero }));
        AssertParameter(nameof(DynamicScalingOptions.ScaleUpInterval),
            OptionsWithScaling(new DynamicScalingOptions { ScaleUpInterval = TimeSpan.FromTicks(-1) }));
        AssertParameter(nameof(DynamicScalingOptions.CapacityMeasurementWindow),
            OptionsWithScaling(new DynamicScalingOptions { CapacityMeasurementWindow = TimeSpan.Zero }));
        AssertParameter(nameof(DynamicScalingOptions.CapacityRetryInterval),
            OptionsWithScaling(new DynamicScalingOptions { CapacityRetryInterval = TimeSpan.Zero }));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(-0.1)]
    public void CapacityGainToleranceMustBeFiniteAndNonNegative(double value)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                CapacityGainTolerance = value
            });

        AssertParameter(nameof(DynamicScalingOptions.CapacityGainTolerance), options);
    }

    [Fact]
    public void ZeroCapacityGainToleranceIsValid()
    {
        OptionsWithScaling(
            new DynamicScalingOptions
            {
                CapacityGainTolerance = 0
            }).Validate();
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(-0.1)]
    [InlineData(0)]
    [InlineData(1.1)]
    public void ThroughputSmoothingFactorMustBeInOpenClosedUnitInterval(double value)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ThroughputSmoothingFactor = value
            });

        AssertParameter(nameof(DynamicScalingOptions.ThroughputSmoothingFactor), options);
    }

    [Fact]
    public void ThroughputSmoothingFactorOfOneIsValid()
    {
        OptionsWithScaling(
            new DynamicScalingOptions
            {
                ThroughputSmoothingFactor = 1
            }).Validate();
    }

    [Fact]
    public void ScaleDownIdleDurationMustNotBeNegative()
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ScaleDownIdleDuration = TimeSpan.FromTicks(-1)
            });

        AssertParameter(nameof(DynamicScalingOptions.ScaleDownIdleDuration), options);
    }

    [Fact]
    public void ZeroScaleUpIntervalAndIdleDurationAreValid()
    {
        OptionsWithScaling(
            new DynamicScalingOptions
            {
                ScaleUpInterval = TimeSpan.Zero,
                ScaleDownIdleDuration = TimeSpan.Zero
            }).Validate();
    }

    [Fact]
    public void MaxParallelismBelowParallelismIsRejected()
    {
        var options = new DispatcherOptions
        {
            Parallelism = 4,
            MaxParallelism = 3
        };

        AssertParameter(nameof(DispatcherOptions.MaxParallelism), options);
    }

    [Fact]
    public void FixedParallelismBoundariesAreValid()
    {
        new DispatcherOptions { Parallelism = 4, MaxParallelism = 0 }.Validate();
        new DispatcherOptions { Parallelism = 4, MaxParallelism = 4 }.Validate();
    }

    private static DispatcherOptions OptionsWithScaling(DynamicScalingOptions dynamicScaling)
    {
        return new DispatcherOptions
        {
            DynamicScaling = dynamicScaling
        };
    }

    private static void AssertParameter(string parameterName, DispatcherOptions options)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(parameterName, exception.ParamName);
    }
}
