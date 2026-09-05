using Xunit;

namespace MessageDispatching.Tests;

public sealed class DispatcherOptionsTests
{
    [Fact]
    public void DefaultsAreValid()
    {
        var options = new DispatcherOptions();

        Assert.Equal(TimeSpan.FromMilliseconds(200), options.ScaleInterval);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ScaleObservationWindow);
        Assert.Equal(0.80, options.ScaleUpSaturationThreshold);
        Assert.Equal(0.70, options.ScaleDownUtilizationThreshold);
        Assert.Equal(TimeSpan.FromSeconds(1), options.ScaleUpCooldown);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ScaleDownCooldown);
        options.Validate();
    }

    [Fact]
    public void ObservationWindowMustContainAtLeastTwoSamples()
    {
        var options = new DispatcherOptions
        {
            ScaleInterval = TimeSpan.FromMilliseconds(100),
            ScaleObservationWindow = TimeSpan.FromMilliseconds(199)
        };

        AssertParameter(nameof(DispatcherOptions.ScaleObservationWindow), options);
    }

    [Fact]
    public void ObservationWindowRejectsExcessiveSampleCounts()
    {
        var options = new DispatcherOptions
        {
            ScaleInterval = TimeSpan.FromTicks(1),
            ScaleObservationWindow = TimeSpan.FromTicks(1_000_001)
        };

        AssertParameter(nameof(DispatcherOptions.ScaleObservationWindow), options);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(0d)]
    [InlineData(-0.1d)]
    [InlineData(1.1d)]
    public void InvalidScaleUpSaturationThresholdIsRejected(double value)
    {
        var options = new DispatcherOptions
        {
            ScaleUpSaturationThreshold = value
        };

        AssertParameter(nameof(DispatcherOptions.ScaleUpSaturationThreshold), options);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(-0.1d)]
    [InlineData(1d)]
    [InlineData(1.1d)]
    public void InvalidScaleDownUtilizationThresholdIsRejected(double value)
    {
        var options = new DispatcherOptions
        {
            ScaleDownUtilizationThreshold = value
        };

        AssertParameter(nameof(DispatcherOptions.ScaleDownUtilizationThreshold), options);
    }

    [Fact]
    public void ScaleThresholdsMustHaveHysteresis()
    {
        var options = new DispatcherOptions
        {
            ScaleUpSaturationThreshold = 0.7,
            ScaleDownUtilizationThreshold = 0.7
        };

        AssertParameter(nameof(DispatcherOptions.ScaleDownUtilizationThreshold), options);
    }

    [Theory]
    [InlineData(-1)]
    public void ScaleCooldownsMustNotBeNegative(int milliseconds)
    {
        AssertParameter(
            nameof(DispatcherOptions.ScaleUpCooldown),
            new DispatcherOptions { ScaleUpCooldown = TimeSpan.FromMilliseconds(milliseconds) });
        AssertParameter(
            nameof(DispatcherOptions.ScaleDownCooldown),
            new DispatcherOptions { ScaleDownCooldown = TimeSpan.FromMilliseconds(milliseconds) });
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

    private static void AssertParameter(string parameterName, DispatcherOptions options)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(parameterName, exception.ParamName);
    }
}
