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
        Assert.Equal(0.02, options.DynamicScaling.MinimumUsefulThroughputGain);
        Assert.Equal(0.25, options.DynamicScaling.ThroughputSmoothingFactor);
        Assert.Equal(1, options.DynamicScaling.ProbeWarmupSamples);
        Assert.Equal(3, options.DynamicScaling.ThroughputMeasurementSamples);
        Assert.Equal(TimeSpan.FromSeconds(10), options.DynamicScaling.ProbeTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), options.DynamicScaling.ScaleUpCooldown);
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

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(-0.1)]
    public void MinimumUsefulThroughputGainMustBeFiniteAndNonNegative(double value)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                MinimumUsefulThroughputGain = value
            });

        AssertParameter(nameof(DynamicScalingOptions.MinimumUsefulThroughputGain), options);
    }

    [Fact]
    public void ZeroMinimumUsefulThroughputGainIsValid()
    {
        OptionsWithScaling(
            new DynamicScalingOptions
            {
                MinimumUsefulThroughputGain = 0
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
    public void ProbeWarmupSamplesMustNotBeNegative()
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ProbeWarmupSamples = -1
            });

        AssertParameter(nameof(DynamicScalingOptions.ProbeWarmupSamples), options);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ThroughputMeasurementSamplesMustBePositive(int value)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ThroughputMeasurementSamples = value
            });

        AssertParameter(nameof(DynamicScalingOptions.ThroughputMeasurementSamples), options);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ProbeTimeoutMustBePositive(long ticks)
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ProbeTimeout = TimeSpan.FromTicks(ticks)
            });

        AssertParameter(nameof(DynamicScalingOptions.ProbeTimeout), options);
    }

    [Fact]
    public void ScaleUpCooldownMustNotBeNegative()
    {
        var options = OptionsWithScaling(
            new DynamicScalingOptions
            {
                ScaleUpCooldown = TimeSpan.FromTicks(-1)
            });

        AssertParameter(nameof(DynamicScalingOptions.ScaleUpCooldown), options);
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
    public void ZeroCooldownAndIdleDurationAreValid()
    {
        OptionsWithScaling(
            new DynamicScalingOptions
            {
                ScaleUpCooldown = TimeSpan.Zero,
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
