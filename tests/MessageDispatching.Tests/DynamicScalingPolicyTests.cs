using System.Diagnostics;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class DynamicScalingPolicyTests
{
    [Fact]
    public void WindowMustFillBeforeScaling()
    {
        var policy = CreatePolicy();

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 0, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 100, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 200, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 300, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.ScaleUp, Observe(policy, 400, 2, 2, 1));
    }

    [Fact]
    public void RepeatedLowDutyBurstsDoNotAccumulateScaleUpDecisions()
    {
        var policy = CreatePolicy();

        for (var sample = 0; sample < 40; sample++)
        {
            var saturated = sample % 4 == 0;
            Assert.Equal(
                DynamicScaleDecision.None,
                Observe(
                    policy,
                    sample * 100,
                    2,
                    saturated ? 2 : 1,
                    saturated ? 1 : 0));
        }
    }

    [Fact]
    public void LatestSampleMustStillBeSaturatedForScaleUp()
    {
        var policy = CreatePolicy(upThreshold: 0.75);

        Observe(policy, 0, 2, 2, 1);
        Observe(policy, 100, 2, 2, 1);
        Observe(policy, 200, 2, 2, 1);
        Observe(policy, 300, 2, 2, 1);

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 400, 2, 1, 0));
    }

    [Fact]
    public void DelayedTicksExpireStaleTimeInsteadOfWeightingSamplesEqually()
    {
        var policy = CreatePolicy(upThreshold: 0.75);

        Observe(policy, 0, 2, 2, 1);
        Observe(policy, 100, 2, 2, 1);
        Observe(policy, 200, 2, 1, 0);

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 500, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 600, 2, 2, 1));
    }

    [Fact]
    public void CoalescedTimerTicksCanExceedTheNominalRingCapacity()
    {
        var options = new DispatcherOptions
        {
            Parallelism = 1,
            MaxParallelism = 4,
            ScaleInterval = TimeSpan.FromMilliseconds(20),
            ScaleObservationWindow = TimeSpan.FromMilliseconds(100),
            ScaleUpSaturationThreshold = 0.80,
            ScaleDownUtilizationThreshold = 0.70,
            ScaleUpCooldown = TimeSpan.Zero,
            ScaleDownCooldown = TimeSpan.Zero
        };
        options.Validate();
        var policy = new DynamicScalingPolicy(options);
        var timestamps = new[]
        {
            0d,
            18.941,
            73.909,
            74.136,
            77.988,
            98.308,
            117.439,
            137.810,
            158.064
        };

        var decision = DynamicScaleDecision.None;
        foreach (var milliseconds in timestamps)
        {
            decision = policy.Observe(1, 1, 1, false, Timestamp(milliseconds));
        }

        Assert.Equal(DynamicScaleDecision.ScaleUp, decision);
    }

    [Fact]
    public void SustainedLowUtilizationScalesDownWithoutCompleteIdleness()
    {
        var policy = CreatePolicy(downThreshold: 0.5);

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 0, 4, 2, 0));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 100, 4, 2, 0));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 200, 4, 2, 0));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 300, 4, 2, 0));
        Assert.Equal(DynamicScaleDecision.ScaleDown, Observe(policy, 400, 4, 2, 0));
    }

    [Fact]
    public void ZeroUtilizationThresholdScalesDownAfterBusySamplesExpire()
    {
        var policy = CreatePolicy(downThreshold: 0);

        for (var milliseconds = 0; milliseconds <= 400; milliseconds += 100)
        {
            Assert.NotEqual(
                DynamicScaleDecision.ScaleDown,
                Observe(policy, milliseconds, 3, 1, 0));
        }

        for (var milliseconds = 500; milliseconds < 900; milliseconds += 100)
        {
            Assert.Equal(
                DynamicScaleDecision.None,
                Observe(policy, milliseconds, 3, 0, 0));
        }

        Assert.Equal(DynamicScaleDecision.ScaleDown, Observe(policy, 900, 3, 0, 0));
    }

    [Fact]
    public void LatestSampleMustHaveSpareCapacityAndNoQueueForScaleDown()
    {
        var policy = CreatePolicy(upThreshold: 1, downThreshold: 0.75);

        Observe(policy, 0, 4, 1, 0);
        Observe(policy, 100, 4, 1, 0);
        Observe(policy, 200, 4, 1, 0);
        Observe(policy, 300, 4, 1, 0);

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 400, 4, 4, 1));
    }

    [Fact]
    public void ScaleChangeCooldownAppliesToBothDirections()
    {
        var policy = CreatePolicy(
            upThreshold: 0.5,
            downThreshold: 0.25,
            upCooldown: TimeSpan.FromMilliseconds(500),
            downCooldown: TimeSpan.FromMilliseconds(700));

        for (var milliseconds = 0; milliseconds <= 300; milliseconds += 100)
        {
            Observe(policy, milliseconds, 2, 2, 1);
        }

        policy.RecordScaleChange(Timestamp(300));

        for (var milliseconds = 400; milliseconds < 800; milliseconds += 100)
        {
            Assert.Equal(
                DynamicScaleDecision.None,
                Observe(policy, milliseconds, 3, 3, 1));
        }

        Assert.Equal(DynamicScaleDecision.ScaleUp, Observe(policy, 800, 3, 3, 1));
        policy.RecordScaleChange(Timestamp(800));

        for (var milliseconds = 900; milliseconds < 1500; milliseconds += 100)
        {
            Assert.Equal(
                DynamicScaleDecision.None,
                Observe(policy, milliseconds, 3, 0, 0));
        }

        Assert.Equal(DynamicScaleDecision.ScaleDown, Observe(policy, 1500, 3, 0, 0));
    }

    [Fact]
    public void PendingRetirementSuppressesAdditionalDecisions()
    {
        var policy = CreatePolicy(upThreshold: 0.5, downThreshold: 0.25);

        Observe(policy, 0, 2, 2, 1);
        Observe(policy, 100, 2, 2, 1);
        Observe(policy, 200, 2, 2, 1);
        Observe(policy, 300, 2, 2, 1);

        Assert.Equal(
            DynamicScaleDecision.None,
            policy.Observe(2, 2, 1, true, Timestamp(400)));
    }

    [Fact]
    public void LongSamplingGapClearsTheWindow()
    {
        var policy = CreatePolicy();

        Observe(policy, 0, 2, 2, 1);
        Observe(policy, 100, 2, 2, 1);
        Observe(policy, 200, 2, 2, 1);

        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 700, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 800, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 900, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.None, Observe(policy, 1000, 2, 2, 1));
        Assert.Equal(DynamicScaleDecision.ScaleUp, Observe(policy, 1100, 2, 2, 1));
    }

    [Fact]
    public void InvalidGaugeValuesAreClamped()
    {
        var policy = CreatePolicy(upThreshold: 1, downThreshold: 0.5);

        Observe(policy, 0, 2, 99, 1);
        Observe(policy, 100, 2, 99, 1);
        Observe(policy, 200, 2, 99, 1);
        Observe(policy, 300, 2, 99, 1);
        Assert.Equal(DynamicScaleDecision.ScaleUp, Observe(policy, 400, 2, 99, 1));

        var downPolicy = CreatePolicy(upThreshold: 1, downThreshold: 0.5);
        Observe(downPolicy, 0, 2, -1, -1);
        Observe(downPolicy, 100, 2, -1, -1);
        Observe(downPolicy, 200, 2, -1, -1);
        Observe(downPolicy, 300, 2, -1, -1);
        Assert.Equal(DynamicScaleDecision.ScaleDown, Observe(downPolicy, 400, 2, -1, -1));
    }

    [Fact]
    public void FloorAndCeilingPreventScalingBeyondConfiguredBounds()
    {
        var upPolicy = CreatePolicy(
            parallelism: 1,
            maxParallelism: 2,
            upThreshold: 0.5,
            downThreshold: 0.25);
        var downPolicy = CreatePolicy(parallelism: 2, maxParallelism: 4, downThreshold: 0.5);

        for (var milliseconds = 0; milliseconds <= 400; milliseconds += 100)
        {
            Assert.NotEqual(
                DynamicScaleDecision.ScaleUp,
                Observe(upPolicy, milliseconds, 2, 2, 1));
            Assert.NotEqual(
                DynamicScaleDecision.ScaleDown,
                Observe(downPolicy, milliseconds, 2, 0, 0));
        }
    }

    private static DynamicScalingPolicy CreatePolicy(
        int parallelism = 1,
        int maxParallelism = 4,
        double upThreshold = 0.75,
        double downThreshold = 0.5,
        TimeSpan? upCooldown = null,
        TimeSpan? downCooldown = null)
    {
        var options = new DispatcherOptions
        {
            Parallelism = parallelism,
            MaxParallelism = maxParallelism,
            ScaleInterval = TimeSpan.FromMilliseconds(100),
            ScaleObservationWindow = TimeSpan.FromMilliseconds(400),
            ScaleUpSaturationThreshold = upThreshold,
            ScaleDownUtilizationThreshold = downThreshold,
            ScaleUpCooldown = upCooldown ?? TimeSpan.Zero,
            ScaleDownCooldown = downCooldown ?? TimeSpan.Zero
        };
        options.Validate();
        return new DynamicScalingPolicy(options);
    }

    private static DynamicScaleDecision Observe(
        DynamicScalingPolicy policy,
        int milliseconds,
        int workers,
        int busy,
        int queued)
    {
        return policy.Observe(workers, busy, queued, false, Timestamp(milliseconds));
    }

    private static long Timestamp(double milliseconds)
    {
        return (long)(Stopwatch.Frequency * (milliseconds / 1000d));
    }
}
