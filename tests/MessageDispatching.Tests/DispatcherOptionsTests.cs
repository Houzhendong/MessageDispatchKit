using Xunit;

namespace MessageDispatching.Tests;

public sealed class DispatcherOptionsTests
{
    [Fact]
    public void ScaleUpQueuedWorkItemsThresholdDefaultsToZeroAndIsValid()
    {
        var options = new DispatcherOptions();

        Assert.Equal(0, options.ScaleUpQueuedWorkItemsThreshold);
        options.Validate();
    }

    [Fact]
    public void NegativeScaleUpQueuedWorkItemsThresholdIsRejected()
    {
        var options = new DispatcherOptions
        {
            ScaleUpQueuedWorkItemsThreshold = -1
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);

        Assert.Equal(nameof(DispatcherOptions.ScaleUpQueuedWorkItemsThreshold), exception.ParamName);
        Assert.Contains("zero or greater", exception.Message);
    }
}
