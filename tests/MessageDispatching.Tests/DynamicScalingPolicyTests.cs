using System.Diagnostics;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class DynamicScalingPolicyTests
{
    [Fact]
    public void FirstSampleBaselinesAndThroughputUsesActualElapsedTime()
    {
        var policy = CreatePolicy(maxParallelism: 1);

        var baseline = Observe(policy, 10, 100, workers: 1, desired: 1, busy: 0, ready: 0);
        var first = Observe(policy, 10.5, 150, workers: 1, desired: 1, busy: 0, ready: 0);
        var second = Observe(policy, 12, 300, workers: 1, desired: 1, busy: 0, ready: 0);

        Assert.Equal(0, baseline.Throughput);
        Assert.Equal(0, baseline.SmoothedThroughput);
        Assert.Equal(100, first.Throughput, 10);
        Assert.Equal(100, first.SmoothedThroughput, 10);
        Assert.Equal(100, second.Throughput, 10);
    }

    [Fact]
    public void EwmaStartsWithFirstValidRawSampleThenBlends()
    {
        var policy = CreatePolicy(maxParallelism: 1, smoothingFactor: 0.25);

        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        var first = Observe(policy, 1, 100, workers: 1, desired: 1, busy: 0, ready: 0);
        var second = Observe(policy, 2, 300, workers: 1, desired: 1, busy: 0, ready: 0);

        Assert.Equal(100, first.Throughput, 10);
        Assert.Equal(100, first.SmoothedThroughput, 10);
        Assert.Equal(200, second.Throughput, 10);
        Assert.Equal(125, second.SmoothedThroughput, 10);
    }

    [Theory]
    [InlineData(0, 0, 1, false)]
    [InlineData(2, 1, 1, false)]
    [InlineData(2, 2, 0, false)]
    [InlineData(2, 2, 1, true)]
    [InlineData(2, 3, 1, true)]
    public void SaturationUsesActualWorkersBusyWorkersAndRunnableWork(
        int workers,
        int busy,
        long ready,
        bool expected)
    {
        var policy = CreatePolicy();

        var result = Observe(policy, 0, 0, workers, workers, busy, ready);

        Assert.Equal(expected, result.IsSaturated);
    }

    [Fact]
    public void PositiveBaselineUsesProportionalScaleUpStep()
    {
        var policy = CreatePolicy(maxParallelism: 32, smoothingFactor: 1);

        Observe(policy, 0, 0, workers: 8, desired: 8, busy: 8, ready: 20);
        var result = Observe(policy, 1, 800, workers: 8, desired: 8, busy: 8, ready: 20);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(10, result.DesiredWorkerCount);
        Assert.Equal(ScalingState.ProbeConvergence, policy.State);
    }

    [Fact]
    public void ZeroBaselineUsesSingleWorkerScaleUpStep()
    {
        var policy = CreatePolicy(maxParallelism: 32, smoothingFactor: 1);

        Observe(policy, 0, 0, workers: 8, desired: 8, busy: 8, ready: 20);
        var result = Observe(policy, 1, 0, workers: 8, desired: 8, busy: 8, ready: 20);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(9, result.DesiredWorkerCount);
        Assert.Equal(0, result.SmoothedThroughput);
    }

    [Theory]
    [InlineData(9, 20, 9)]
    [InlineData(32, 1, 9)]
    public void ScaleUpTargetIsCappedByMaximumAndRunnableParallelism(
        int maxParallelism,
        long ready,
        int expectedTarget)
    {
        var policy = CreatePolicy(maxParallelism: maxParallelism, smoothingFactor: 1);

        Observe(policy, 0, 0, workers: 8, desired: 8, busy: 8, ready: ready);
        var result = Observe(policy, 1, 800, workers: 8, desired: 8, busy: 8, ready: ready);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(expectedTarget, result.DesiredWorkerCount);
    }

    [Theory]
    [InlineData(8, 9, 8, 20, 32)]
    [InlineData(8, 8, 7, 20, 32)]
    [InlineData(8, 8, 8, 0, 32)]
    [InlineData(8, 8, 8, 20, 8)]
    public void StableProbeRequiresConvergenceSaturationAndAvailableCapacity(
        int workers,
        int desired,
        int busy,
        long ready,
        int maxParallelism)
    {
        var policy = CreatePolicy(maxParallelism: maxParallelism, smoothingFactor: 1);

        Observe(policy, 0, 0, workers, desired, busy, ready);
        var result = Observe(policy, 1, 100, workers, desired, busy, ready);

        Assert.Equal(ScalingReason.None, result.Reason);
        Assert.Equal(desired, result.DesiredWorkerCount);
        Assert.Equal(ScalingState.Stable, policy.State);
    }

    [Fact]
    public void ProbeWaitsForLaterTargetConvergenceWarmupAndMeasurementSamples()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            minimumGain: 0.5,
            smoothingFactor: 1,
            warmupSamples: 2);

        Observe(policy, 0, 0, workers: 2, desired: 2, busy: 2, ready: 4);
        var started = Observe(policy, 1, 100, workers: 2, desired: 2, busy: 2, ready: 4);
        var waiting = Observe(policy, 2, 200, workers: 2, desired: 3, busy: 2, ready: 4);
        var converged = Observe(policy, 3, 300, workers: 3, desired: 3, busy: 3, ready: 4);
        var warmup1 = Observe(policy, 4, 450, workers: 3, desired: 3, busy: 3, ready: 4);
        var warmup2 = Observe(policy, 5, 600, workers: 3, desired: 3, busy: 3, ready: 4);
        var measured = Observe(policy, 6, 750, workers: 3, desired: 3, busy: 3, ready: 4);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(ScalingReason.None, waiting.Reason);
        Assert.Equal(ScalingReason.None, converged.Reason);
        Assert.Equal(ScalingReason.None, warmup1.Reason);
        Assert.Equal(ScalingReason.None, warmup2.Reason);
        Assert.Equal(ScalingReason.ProbeAccepted, measured.Reason);
        Assert.Equal(3, measured.DesiredWorkerCount);
        Assert.Equal(0.5, measured.ProbeGain!.Value, 10);
        Assert.Equal(ScalingState.Stable, policy.State);
    }

    [Fact]
    public void PositiveBaselineRejectsGainBelowThreshold()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            minimumGain: 0.02,
            smoothingFactor: 1,
            warmupSamples: 0);

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        var result = Observe(policy, 3, 301, workers: 3, desired: 3, busy: 3, ready: 2);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Equal(0.01, result.ProbeGain!.Value, 10);
        Assert.Equal(ScalingState.RollbackConvergence, policy.State);
    }

    [Fact]
    public void NegativeProbeGainIsRejected()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            minimumGain: 0.02,
            smoothingFactor: 1,
            warmupSamples: 0);

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        var result = Observe(policy, 3, 280, workers: 3, desired: 3, busy: 3, ready: 2);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(-0.2, result.ProbeGain!.Value, 10);
        Assert.Equal(2, result.DesiredWorkerCount);
    }

    [Fact]
    public void ProbeIsRejectedAsInconclusiveWhenRunnableParallelismFallsBelowTarget()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            minimumGain: 0.02,
            smoothingFactor: 1,
            warmupSamples: 0);

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        var result = Observe(policy, 3, 400, workers: 3, desired: 3, busy: 1, ready: 1);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Null(result.ProbeGain);
    }

    [Fact]
    public void ZeroBaselineAcceptsOnlyPositiveProbeThroughputWithoutGain()
    {
        var acceptedPolicy = CreatePolicy(
            maxParallelism: 8,
            smoothingFactor: 1,
            warmupSamples: 0);
        var rejectedPolicy = CreatePolicy(
            maxParallelism: 8,
            smoothingFactor: 1,
            warmupSamples: 0);

        StartZeroBaselineTwoToThreeWorkerProbe(acceptedPolicy);
        Observe(acceptedPolicy, 2, 0, workers: 3, desired: 3, busy: 3, ready: 2);
        var accepted = Observe(
            acceptedPolicy,
            3,
            1,
            workers: 3,
            desired: 3,
            busy: 3,
            ready: 2);

        StartZeroBaselineTwoToThreeWorkerProbe(rejectedPolicy);
        Observe(rejectedPolicy, 2, 0, workers: 3, desired: 3, busy: 3, ready: 2);
        var rejected = Observe(
            rejectedPolicy,
            3,
            0,
            workers: 3,
            desired: 3,
            busy: 3,
            ready: 2);

        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(3, accepted.DesiredWorkerCount);
        Assert.Null(accepted.ProbeGain);
        Assert.Equal(ScalingReason.ProbeRejected, rejected.Reason);
        Assert.Equal(2, rejected.DesiredWorkerCount);
        Assert.Null(rejected.ProbeGain);
    }

    [Fact]
    public void RejectedProbeCooldownStartsOnlyAfterRollbackConverges()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0,
            scaleUpCooldown: TimeSpan.FromSeconds(2));

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        var rejected = Observe(policy, 3, 300, workers: 3, desired: 3, busy: 3, ready: 2);
        var rollingBack = Observe(policy, 4, 400, workers: 3, desired: 2, busy: 3, ready: 2);
        var converged = Observe(policy, 5, 500, workers: 2, desired: 2, busy: 2, ready: 4);
        var beforeCooldown = Observe(
            policy,
            6.9,
            690,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 4);
        var atCooldown = Observe(
            policy,
            7,
            700,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 4);

        Assert.Equal(ScalingReason.ProbeRejected, rejected.Reason);
        Assert.Equal(ScalingReason.None, rollingBack.Reason);
        Assert.Equal(ScalingReason.None, converged.Reason);
        Assert.Equal(ScalingReason.None, beforeCooldown.Reason);
        Assert.Equal(ScalingReason.ProbeUp, atCooldown.Reason);
    }

    [Fact]
    public void ScaleUpCooldownDoesNotBlockIdleScaleDown()
    {
        var policy = CreatePolicy(
            parallelism: 1,
            maxParallelism: 8,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0,
            scaleUpCooldown: TimeSpan.FromSeconds(10),
            scaleDownIdleDuration: TimeSpan.FromSeconds(2));

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        Observe(policy, 3, 300, workers: 3, desired: 3, busy: 3, ready: 2);
        Observe(policy, 4, 300, workers: 2, desired: 2, busy: 0, ready: 0);

        var beforeIdleDelay = Observe(
            policy,
            5.9,
            300,
            workers: 2,
            desired: 2,
            busy: 0,
            ready: 0);
        var result = Observe(
            policy,
            6,
            300,
            workers: 2,
            desired: 2,
            busy: 0,
            ready: 0);

        Assert.Equal(ScalingReason.None, beforeIdleDelay.Reason);
        Assert.Equal(ScalingReason.IdleScaleDown, result.Reason);
        Assert.Equal(1, result.DesiredWorkerCount);
    }

    [Fact]
    public void IdleScaleDownRequiresContinuousDelayResetsAfterConvergenceAndStopsAtFloor()
    {
        var policy = CreatePolicy(
            parallelism: 2,
            maxParallelism: 8,
            scaleDownIdleDuration: TimeSpan.FromSeconds(3));

        Observe(policy, 0, 0, workers: 4, desired: 4, busy: 1, ready: 0);
        Assert.Equal(
            ScalingReason.None,
            Observe(policy, 2, 0, workers: 4, desired: 4, busy: 1, ready: 0).Reason);

        Observe(policy, 2.5, 0, workers: 4, desired: 4, busy: 1, ready: 1);
        Observe(policy, 3, 0, workers: 4, desired: 4, busy: 1, ready: 0);
        Assert.Equal(
            ScalingReason.None,
            Observe(policy, 5.9, 0, workers: 4, desired: 4, busy: 1, ready: 0).Reason);

        var firstScaleDown = Observe(
            policy,
            6,
            0,
            workers: 4,
            desired: 4,
            busy: 1,
            ready: 0);
        Assert.Equal(ScalingReason.IdleScaleDown, firstScaleDown.Reason);
        Assert.Equal(3, firstScaleDown.DesiredWorkerCount);

        Assert.Equal(
            ScalingReason.None,
            Observe(policy, 7, 0, workers: 4, desired: 3, busy: 1, ready: 0).Reason);
        Observe(policy, 8, 0, workers: 3, desired: 3, busy: 1, ready: 0);
        Assert.Equal(
            ScalingReason.None,
            Observe(policy, 10.9, 0, workers: 3, desired: 3, busy: 1, ready: 0).Reason);

        var secondScaleDown = Observe(
            policy,
            11,
            0,
            workers: 3,
            desired: 3,
            busy: 1,
            ready: 0);
        Assert.Equal(ScalingReason.IdleScaleDown, secondScaleDown.Reason);
        Assert.Equal(2, secondScaleDown.DesiredWorkerCount);

        Observe(policy, 12, 0, workers: 2, desired: 2, busy: 0, ready: 0);
        var atFloor = Observe(policy, 20, 0, workers: 2, desired: 2, busy: 0, ready: 0);

        Assert.Equal(ScalingReason.None, atFloor.Reason);
        Assert.Equal(2, atFloor.DesiredWorkerCount);
    }

    [Fact]
    public void InvalidSamplesRebaselineWithoutUpdatingEwma()
    {
        var policy = CreatePolicy(maxParallelism: 1, smoothingFactor: 0.5);

        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        var valid = Observe(policy, 1, 100, workers: 1, desired: 1, busy: 0, ready: 0);
        var duplicateTimestamp = Observe(
            policy,
            1,
            200,
            workers: 1,
            desired: 1,
            busy: 0,
            ready: 0);
        var afterTimestampRebaseline = Observe(
            policy,
            2,
            300,
            workers: 1,
            desired: 1,
            busy: 0,
            ready: 0);
        var completionRegression = Observe(
            policy,
            3,
            250,
            workers: 1,
            desired: 1,
            busy: 0,
            ready: 0);
        var afterCompletionRebaseline = Observe(
            policy,
            4,
            350,
            workers: 1,
            desired: 1,
            busy: 0,
            ready: 0);

        Assert.Equal(100, valid.SmoothedThroughput, 10);
        Assert.Equal(0, duplicateTimestamp.Throughput);
        Assert.Equal(100, duplicateTimestamp.SmoothedThroughput, 10);
        Assert.Equal(100, afterTimestampRebaseline.Throughput, 10);
        Assert.Equal(100, afterTimestampRebaseline.SmoothedThroughput, 10);
        Assert.Equal(0, completionRegression.Throughput);
        Assert.Equal(100, completionRegression.SmoothedThroughput, 10);
        Assert.Equal(100, afterCompletionRebaseline.Throughput, 10);
    }

    [Fact]
    public void InvalidSampleDuringActiveProbeFailsSafeToRollback()
    {
        var policy = CreatePolicy(
            maxParallelism: 8,
            smoothingFactor: 1,
            warmupSamples: 1);

        StartTwoToThreeWorkerProbe(policy);
        Observe(policy, 2, 200, workers: 3, desired: 3, busy: 3, ready: 2);
        var result = Observe(policy, 2, 250, workers: 3, desired: 3, busy: 3, ready: 2);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Equal(0, result.Throughput);
        Assert.Null(result.ProbeGain);
        Assert.Equal(ScalingState.RollbackConvergence, policy.State);
    }

    [Fact]
    public void InvalidSampleBreaksContinuousIdleDuration()
    {
        var policy = CreatePolicy(
            parallelism: 1,
            maxParallelism: 4,
            scaleDownIdleDuration: TimeSpan.FromSeconds(3));

        Observe(policy, 0, 10, workers: 3, desired: 3, busy: 0, ready: 0);
        Observe(policy, 2, 5, workers: 3, desired: 3, busy: 0, ready: 0);
        var restarted = Observe(policy, 4, 5, workers: 3, desired: 3, busy: 0, ready: 0);
        var beforeDelay = Observe(policy, 6.9, 5, workers: 3, desired: 3, busy: 0, ready: 0);
        var afterDelay = Observe(policy, 7, 5, workers: 3, desired: 3, busy: 0, ready: 0);

        Assert.Equal(ScalingReason.None, restarted.Reason);
        Assert.Equal(ScalingReason.None, beforeDelay.Reason);
        Assert.Equal(ScalingReason.IdleScaleDown, afterDelay.Reason);
        Assert.Equal(2, afterDelay.DesiredWorkerCount);
    }

    private static void StartTwoToThreeWorkerProbe(DynamicScalingPolicy policy)
    {
        Observe(policy, 0, 0, workers: 2, desired: 2, busy: 2, ready: 2);
        var started = Observe(policy, 1, 100, workers: 2, desired: 2, busy: 2, ready: 2);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(3, started.DesiredWorkerCount);
    }

    private static void StartZeroBaselineTwoToThreeWorkerProbe(DynamicScalingPolicy policy)
    {
        Observe(policy, 0, 0, workers: 2, desired: 2, busy: 2, ready: 2);
        var started = Observe(policy, 1, 0, workers: 2, desired: 2, busy: 2, ready: 2);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(3, started.DesiredWorkerCount);
    }

    private static DynamicScalingPolicy CreatePolicy(
        int parallelism = 1,
        int maxParallelism = 16,
        double minimumGain = 0.02,
        double smoothingFactor = 0.25,
        int warmupSamples = 1,
        TimeSpan? scaleUpCooldown = null,
        TimeSpan? scaleDownIdleDuration = null)
    {
        var options = new DispatcherOptions
        {
            Parallelism = parallelism,
            MaxParallelism = maxParallelism,
            DynamicScaling = new DynamicScalingOptions
            {
                SampleInterval = TimeSpan.FromMilliseconds(100),
                MinimumUsefulThroughputGain = minimumGain,
                ThroughputSmoothingFactor = smoothingFactor,
                ProbeWarmupSamples = warmupSamples,
                ScaleUpCooldown = scaleUpCooldown ?? TimeSpan.Zero,
                ScaleDownIdleDuration = scaleDownIdleDuration ?? TimeSpan.FromSeconds(30)
            }
        };
        options.Validate();
        return new DynamicScalingPolicy(options);
    }

    private static ScalingResult Observe(
        DynamicScalingPolicy policy,
        double seconds,
        long completed,
        int workers,
        int desired,
        int busy,
        long ready)
    {
        return policy.Observe(
            new ScalingSnapshot(
                workers,
                desired,
                busy,
                ready,
                completed,
                Timestamp(seconds)));
    }

    private static long Timestamp(double seconds)
    {
        return (long)(Stopwatch.Frequency * seconds);
    }
}
