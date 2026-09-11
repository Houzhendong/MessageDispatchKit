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
    public void PositiveBaselineStartsWithSingleWorkerScaleUpStep()
    {
        var policy = CreatePolicy(maxParallelism: 32, smoothingFactor: 1);

        Observe(policy, 0, 0, workers: 8, desired: 8, busy: 8, ready: 20);
        var result = Observe(policy, 1, 800, workers: 8, desired: 8, busy: 8, ready: 20);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(9, result.DesiredWorkerCount);
        Assert.Equal(1, policy.ActiveProbeStep);
        Assert.Null(policy.NextProbeStep);
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

    [Fact]
    public void StrongAcceptedProbesDoubleNextStepFromOneToTwoThenTwoToFour()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        var secondStarted = Observe(
            policy,
            4,
            500,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var secondAccepted = Observe(
            policy,
            6,
            800,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeUp, secondStarted.Reason);
        Assert.Equal(4, secondStarted.DesiredWorkerCount);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Equal(ScalingReason.ProbeAccepted, secondAccepted.Reason);
        Assert.Equal(4, policy.NextProbeStep);
    }

    [Fact]
    public void ProportionalSafetyCapLimitsStepToHalfAboveTwoWorkers()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongTwoToFourWorkerProbe(policy);
        var result = Observe(
            policy,
            7,
            1000,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 20);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(6, result.DesiredWorkerCount);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Equal(4, policy.NextProbeStep);
    }

    [Fact]
    public void MediumElasticityRetainsActualProbeStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var result = Observe(
            policy,
            6,
            750,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeAccepted, result.Reason);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Equal(2, policy.NextProbeStep);
    }

    [Fact]
    public void WeakAcceptedElasticityHalvesActualProbeStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            minimumGain: 0.05,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var result = Observe(
            policy,
            6,
            710,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeAccepted, result.Reason);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Equal(1, policy.NextProbeStep);
    }

    [Fact]
    public void PositiveInsufficientGainRejectsAndHalvesActualProbeStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var result = Observe(
            policy,
            6,
            705,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Equal(1, policy.NextProbeStep);
    }

    [Fact]
    public void NegativeAndNullProbeGainsResetNextStep()
    {
        var negativePolicy = CreatePolicy(
            maxParallelism: 16,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0);
        var nullPolicy = CreatePolicy(
            maxParallelism: 16,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(negativePolicy);
        Observe(negativePolicy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(negativePolicy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var negative = Observe(
            negativePolicy,
            6,
            680,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        AcceptStrongOneToTwoWorkerProbe(nullPolicy);
        Observe(nullPolicy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(nullPolicy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var inconclusive = Observe(
            nullPolicy,
            6,
            800,
            workers: 4,
            desired: 4,
            busy: 1,
            ready: 1);

        Assert.Equal(ScalingReason.ProbeRejected, negative.Reason);
        Assert.True(negative.ProbeGain < 0);
        Assert.Null(negativePolicy.NextProbeStep);
        Assert.Equal(ScalingReason.ProbeRejected, inconclusive.Reason);
        Assert.Null(inconclusive.ProbeGain);
        Assert.Null(nullPolicy.NextProbeStep);
    }

    [Fact]
    public void ZeroBaselineAlwaysProbesOneWorkerAndAcceptedProbeResetsNextStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        var firstStarted = Observe(
            policy,
            4,
            400,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);
        Observe(policy, 5, 400, workers: 3, desired: 3, busy: 3, ready: 10);
        var accepted = Observe(
            policy,
            6,
            401,
            workers: 3,
            desired: 3,
            busy: 3,
            ready: 10);
        var secondStarted = Observe(
            policy,
            7,
            401,
            workers: 3,
            desired: 3,
            busy: 3,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeUp, firstStarted.Reason);
        Assert.Equal(3, firstStarted.DesiredWorkerCount);
        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Null(accepted.ProbeGain);
        Assert.Null(policy.NextProbeStep);
        Assert.Equal(ScalingReason.ProbeUp, secondStarted.Reason);
        Assert.Equal(4, secondStarted.DesiredWorkerCount);
        Assert.Equal(1, policy.ActiveProbeStep);
    }

    [Theory]
    [InlineData(5, 20)]
    [InlineData(16, 1)]
    public void MaximumAndRunnableCapsUseActualDeltaForAdaptation(
        int maxParallelism,
        long ready)
    {
        var policy = CreatePolicy(
            maxParallelism: maxParallelism,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongTwoToFourWorkerProbe(policy);
        var started = Observe(
            policy,
            7,
            1000,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: ready);
        Observe(policy, 8, 1200, workers: 5, desired: 5, busy: 5, ready: 20);
        var accepted = Observe(
            policy,
            9,
            1500,
            workers: 5,
            desired: 5,
            busy: 5,
            ready: 20);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(5, started.DesiredWorkerCount);
        Assert.Equal(1, policy.ActiveProbeStep);
        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(2, policy.NextProbeStep);
    }

    [Fact]
    public void MultiworkerRejectionRollsBackWholeStepBeforeAdjustedProbeAfterCooldown()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            minimumGain: 0.1,
            smoothingFactor: 1,
            warmupSamples: 0,
            scaleUpCooldown: TimeSpan.FromSeconds(2));

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var rejected = Observe(
            policy,
            6,
            705,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);
        var rollingBack = Observe(
            policy,
            7,
            810,
            workers: 4,
            desired: 2,
            busy: 4,
            ready: 10);
        var converged = Observe(
            policy,
            8,
            915,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);
        var beforeCooldown = Observe(
            policy,
            9.9,
            1114,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);
        var afterCooldown = Observe(
            policy,
            10,
            1125,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeRejected, rejected.Reason);
        Assert.Equal(2, rejected.DesiredWorkerCount);
        Assert.Equal(1, policy.NextProbeStep);
        Assert.Equal(ScalingReason.None, rollingBack.Reason);
        Assert.Equal(2, rollingBack.DesiredWorkerCount);
        Assert.Equal(ScalingReason.None, converged.Reason);
        Assert.Equal(ScalingReason.None, beforeCooldown.Reason);
        Assert.Equal(ScalingReason.ProbeUp, afterCooldown.Reason);
        Assert.Equal(3, afterCooldown.DesiredWorkerCount);
        Assert.Equal(1, policy.ActiveProbeStep);
    }

    [Fact]
    public void IdleScaleDownResetsNextProbeStep()
    {
        var policy = CreatePolicy(
            parallelism: 1,
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0,
            scaleDownIdleDuration: TimeSpan.FromSeconds(2));

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 400, workers: 2, desired: 2, busy: 0, ready: 0);
        var result = Observe(
            policy,
            6,
            400,
            workers: 2,
            desired: 2,
            busy: 0,
            ready: 0);

        Assert.Equal(ScalingReason.IdleScaleDown, result.Reason);
        Assert.Equal(1, result.DesiredWorkerCount);
        Assert.Null(policy.NextProbeStep);
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
            maxParallelism: 3,
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
    public void InvalidSampleDuringActiveProbeFailsSafeToRollbackAndResetsNextStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 16,
            smoothingFactor: 1,
            warmupSamples: 0);

        AcceptStrongOneToTwoWorkerProbe(policy);
        Observe(policy, 4, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        var result = Observe(
            policy,
            4,
            550,
            workers: 2,
            desired: 4,
            busy: 2,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Equal(0, result.Throughput);
        Assert.Null(result.ProbeGain);
        Assert.Equal(2, policy.ActiveProbeStep);
        Assert.Null(policy.NextProbeStep);
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

    [Fact]
    public void DefaultWindowsRejectConstantThroughputAfterIdle()
    {
        var policy = new DynamicScalingPolicy(new DispatcherOptions
        {
            Parallelism = 1,
            MaxParallelism = 16
        });
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        Observe(policy, 0.5, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        var workers = 1;
        long completed = 0;
        var rejected = 0;

        for (var tick = 2; tick <= 40; tick++)
        {
            completed += 50;
            var result = Observe(policy, tick * 0.5, completed, workers, workers, workers, 1000);
            Assert.NotEqual(ScalingReason.ProbeAccepted, result.Reason);
            if (result.Reason == ScalingReason.ProbeRejected)
            {
                Assert.Equal(0, result.ProbeGain!.Value, 10);
                rejected++;
            }

            workers = result.DesiredWorkerCount;
            Assert.InRange(workers, 1, 2);
        }

        Assert.True(rejected > 0);
    }

    [Fact]
    public void DefaultWindowsAllowLinearScalingFrom64To128Workers()
    {
        var policy = new DynamicScalingPolicy(new DispatcherOptions
        {
            Parallelism = 64,
            MaxParallelism = 128
        });
        var workers = 64;
        long completed = 0;
        var accepted = 0;
        int? firstTarget = null;
        Observe(policy, 0, completed, workers, workers, workers, 1000);

        for (var tick = 1; tick <= 60; tick++)
        {
            completed += workers * 50;
            var result = Observe(policy, tick * 0.5, completed, workers, workers, workers, 1000);
            Assert.NotEqual(ScalingReason.ProbeRejected, result.Reason);
            if (result.Reason == ScalingReason.ProbeUp)
            {
                firstTarget ??= result.DesiredWorkerCount;
            }
            else if (result.Reason == ScalingReason.ProbeAccepted)
            {
                accepted++;
            }

            workers = result.DesiredWorkerCount;
            Assert.InRange(workers, 64, 128);
        }

        Assert.Equal(67, firstTarget);
        Assert.True(accepted > 0);
        Assert.Equal(128, workers);
    }

    [Fact]
    public void MeasurementWindowsWeightCompletionsByActualElapsedTime()
    {
        var policy = CreatePolicy(measurementSamples: 3, warmupSamples: 0);
        Observe(policy, 0, 0, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 1, 100, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 3, 500, workers: 2, desired: 2, busy: 2, ready: 10);
        var started = Observe(policy, 6, 900, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 7, 1500, workers: 3, desired: 3, busy: 3, ready: 10);
        Observe(policy, 8, 1670, workers: 3, desired: 3, busy: 3, ready: 10);
        var measuring = Observe(policy, 10, 2010, workers: 3, desired: 3, busy: 3, ready: 10);
        var measured = Observe(policy, 13, 2520, workers: 3, desired: 3, busy: 3, ready: 10);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(ScalingReason.None, measuring.Reason);
        Assert.Equal(ScalingReason.ProbeAccepted, measured.Reason);
        Assert.Equal((170d - 150) / 150, measured.ProbeGain!.Value, 10);
    }

    [Fact]
    public void ConvergenceAndWarmupCompletionsDoNotEnterProbeWindow()
    {
        var policy = CreatePolicy(measurementSamples: 2, warmupSamples: 2);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 2, 200, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 3, 1000, workers: 1, desired: 2, busy: 1, ready: 10);
        Observe(policy, 4, 3000, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 6000, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 6, 10000, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 7, 10100, workers: 2, desired: 2, busy: 2, ready: 10);
        var result = Observe(policy, 8, 10200, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(0, result.ProbeGain!.Value, 10);
        Assert.True(result.SmoothedThroughput > 100);
    }

    [Fact]
    public void BaselineDiscardsMixedIntervalsAndRestartsAfterLoadInterruption()
    {
        var policy = CreatePolicy(measurementSamples: 2, warmupSamples: 0);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        Observe(policy, 1, 1000, workers: 1, desired: 1, busy: 1, ready: 10);
        var incomplete = Observe(policy, 2, 1100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 3, 1100, workers: 1, desired: 1, busy: 0, ready: 0);
        Observe(policy, 4, 2100, workers: 1, desired: 1, busy: 1, ready: 10);
        var restarted = Observe(policy, 5, 2200, workers: 1, desired: 1, busy: 1, ready: 10);
        var started = Observe(policy, 6, 2300, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 7, 9999, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 8, 10099, workers: 2, desired: 2, busy: 2, ready: 10);
        var measured = Observe(policy, 9, 10199, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.None, restarted.Reason);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(ScalingReason.ProbeRejected, measured.Reason);
        Assert.Equal(0, measured.ProbeGain!.Value, 10);
    }

    [Fact]
    public void BaselineRestartsWhenWorkerConfigurationChanges()
    {
        var policy = CreatePolicy(measurementSamples: 2);
        Observe(policy, 0, 0, workers: 4, desired: 4, busy: 4, ready: 10);
        Observe(policy, 1, 100, workers: 4, desired: 4, busy: 4, ready: 10);
        Observe(policy, 2, 200, workers: 4, desired: 5, busy: 4, ready: 10);
        Observe(policy, 3, 300, workers: 5, desired: 5, busy: 5, ready: 10);
        var incomplete = Observe(policy, 4, 400, workers: 5, desired: 5, busy: 5, ready: 10);
        var started = Observe(policy, 5, 500, workers: 5, desired: 5, busy: 5, ready: 10);

        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(6, started.DesiredWorkerCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ProbeWindowRestartsAfterLoadOrConvergenceInterruption(bool losesConvergence)
    {
        var policy = CreatePolicy(measurementSamples: 2, warmupSamples: 0);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 2, 200, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 3, 300, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 4, 400, workers: 2, desired: 2, busy: 2, ready: 10);
        var interrupted = Observe(
            policy, 5, 500, workers: losesConvergence ? 1 : 2, desired: 2, busy: 1, ready: 10);
        Observe(policy, 6, 600, workers: 2, desired: 2, busy: 2, ready: 10);
        var incomplete = Observe(policy, 7, 700, workers: 2, desired: 2, busy: 2, ready: 10);
        var measured = Observe(policy, 8, 800, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(ScalingReason.None, interrupted.Reason);
        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.ProbeRejected, measured.Reason);
        Assert.Equal(0, measured.ProbeGain!.Value, 10);
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(0.5, 100)]
    [InlineData(2, 50)]
    public void InvalidSamplesDiscardPartialBaseline(double seconds, long completed)
    {
        var policy = CreatePolicy(measurementSamples: 2);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, seconds, completed, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 3, 200, workers: 1, desired: 1, busy: 1, ready: 10);
        var incomplete = Observe(policy, 4, 300, workers: 1, desired: 1, busy: 1, ready: 10);
        var started = Observe(policy, 5, 400, workers: 1, desired: 1, busy: 1, ready: 10);

        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
    }

    [Fact]
    public void AcceptedWindowIsReusedForNextProbe()
    {
        var policy = CreatePolicy(measurementSamples: 3, warmupSamples: 0);
        for (var second = 0; second <= 3; second++)
        {
            Observe(policy, second, second * 100, workers: 1, desired: 1, busy: 1, ready: 10);
        }

        Observe(policy, 4, 400, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 5, 600, workers: 2, desired: 2, busy: 2, ready: 10);
        Observe(policy, 6, 800, workers: 2, desired: 2, busy: 2, ready: 10);
        var accepted = Observe(policy, 7, 1000, workers: 2, desired: 2, busy: 2, ready: 10);
        var next = Observe(policy, 8, 1200, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(1, accepted.ProbeGain!.Value, 10);
        Assert.Equal(ScalingReason.ProbeUp, next.Reason);
        Assert.Equal(4, next.DesiredWorkerCount);
    }

    [Fact]
    public void NegativeProbeRebuildsBaselineAndReseedsStepAfterRollbackAndCooldown()
    {
        var policy = CreatePolicy(
            parallelism: 64, maxParallelism: 128, measurementSamples: 2,
            warmupSamples: 0, scaleUpCooldown: TimeSpan.FromSeconds(2));
        Observe(policy, 0, 0, workers: 64, desired: 64, busy: 64, ready: 100);
        Observe(policy, 1, 6400, workers: 64, desired: 64, busy: 64, ready: 100);
        Observe(policy, 2, 12800, workers: 64, desired: 64, busy: 64, ready: 100);
        Observe(policy, 3, 19500, workers: 67, desired: 67, busy: 67, ready: 100);
        Observe(policy, 4, 25800, workers: 67, desired: 67, busy: 67, ready: 100);
        var rejected = Observe(policy, 5, 32100, workers: 67, desired: 67, busy: 67, ready: 100);
        Assert.Equal(ScalingReason.ProbeRejected, rejected.Reason);
        Assert.True(rejected.ProbeGain < 0);
        Assert.Null(policy.NextProbeStep);

        Observe(policy, 6, 32100, workers: 67, desired: 64, busy: 67, ready: 100);
        Observe(policy, 7, 38500, workers: 64, desired: 64, busy: 64, ready: 100);
        var incomplete = Observe(policy, 8, 44900, workers: 64, desired: 64, busy: 64, ready: 100);
        var restarted = Observe(policy, 9, 51300, workers: 64, desired: 64, busy: 64, ready: 100);

        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.ProbeUp, restarted.Reason);
        Assert.Equal(67, restarted.DesiredWorkerCount);
    }

    [Fact]
    public void InsufficientGainCannotReduceProbeBelowHighParallelismMinimumStep()
    {
        var policy = CreatePolicy(parallelism: 64, maxParallelism: 128, warmupSamples: 0);
        Observe(policy, 0, 0, workers: 64, desired: 64, busy: 64, ready: 100);
        Observe(policy, 1, 6400, workers: 64, desired: 64, busy: 64, ready: 100);
        Observe(policy, 2, 12800, workers: 67, desired: 67, busy: 67, ready: 100);
        var rejected = Observe(policy, 3, 19264, workers: 67, desired: 67, busy: 67, ready: 100);
        Assert.Equal(ScalingReason.ProbeRejected, rejected.Reason);
        Assert.Equal(1, policy.NextProbeStep);

        Observe(policy, 4, 25664, workers: 64, desired: 64, busy: 64, ready: 100);
        var restarted = Observe(policy, 5, 32064, workers: 64, desired: 64, busy: 64, ready: 100);

        Assert.Equal(ScalingReason.ProbeUp, restarted.Reason);
        Assert.Equal(66, restarted.DesiredWorkerCount);
        Assert.Equal(2, policy.ActiveProbeStep);
    }

    [Fact]
    public void IdleScaleDownRebuildsBaselineAndReseedsHighParallelismStep()
    {
        var policy = CreatePolicy(
            maxParallelism: 128, measurementSamples: 2,
            scaleDownIdleDuration: TimeSpan.Zero);
        Observe(policy, 0, 0, workers: 64, desired: 64, busy: 0, ready: 0);
        var scaledDown = Observe(policy, 1, 0, workers: 64, desired: 64, busy: 0, ready: 0);
        Observe(policy, 2, 6300, workers: 63, desired: 63, busy: 63, ready: 100);
        var incomplete = Observe(policy, 3, 12600, workers: 63, desired: 63, busy: 63, ready: 100);
        var started = Observe(policy, 4, 18900, workers: 63, desired: 63, busy: 63, ready: 100);

        Assert.Equal(ScalingReason.IdleScaleDown, scaledDown.Reason);
        Assert.Equal(63, scaledDown.DesiredWorkerCount);
        Assert.Equal(ScalingReason.None, incomplete.Reason);
        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(66, started.DesiredWorkerCount);
        Assert.Null(policy.NextProbeStep);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ProbeTimeoutBoundsConvergenceWarmupAndMeasurement(int phase)
    {
        var policy = CreatePolicy(
            warmupSamples: phase == 1 ? 10 : 0,
            probeTimeout: TimeSpan.FromSeconds(4));
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        var workers = phase == 0 ? 1 : 2;
        var busy = phase == 1 ? 2 : 1;
        Observe(policy, 2, 200, workers, desired: 2, busy, ready: 10);
        var beforeTimeout = Observe(policy, 4.9, 390, workers, desired: 2, busy, ready: 10);
        var expectedState = phase switch
        {
            0 => ScalingState.ProbeConvergence,
            1 => ScalingState.ProbeWarmup,
            _ => ScalingState.ProbeMeasure
        };
        Assert.Equal(expectedState, policy.State);
        var timedOut = Observe(policy, 5, 400, workers, desired: 2, busy, ready: 10);

        Assert.Equal(ScalingReason.None, beforeTimeout.Reason);
        Assert.Equal(ScalingReason.ProbeRejected, timedOut.Reason);
        Assert.Equal(1, timedOut.DesiredWorkerCount);
        Assert.Null(timedOut.ProbeGain);
        Assert.Null(policy.NextProbeStep);
    }

    [Theory]
    [InlineData(66, 100, 66)]
    [InlineData(128, 1, 65)]
    public void HighParallelismInitialStepHonorsMaximumAndRunnableCaps(int maximum, int ready, int target)
    {
        var policy = CreatePolicy(parallelism: 64, maxParallelism: maximum);
        Observe(policy, 0, 0, workers: 64, desired: 64, busy: 64, ready);
        var result = Observe(policy, 1, 6400, workers: 64, desired: 64, busy: 64, ready);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(target, result.DesiredWorkerCount);
        Assert.Equal(target - 64, policy.ActiveProbeStep);
    }

    [Fact]
    public void ExtremeGainAndWorkerCountsDoNotOverflowStepCalculation()
    {
        var policy = CreatePolicy(maxParallelism: int.MaxValue, minimumGain: double.MaxValue);
        const int workers = int.MaxValue - 2;
        Observe(policy, 0, 0, workers, workers, workers, long.MaxValue);
        var result = Observe(policy, 1, 100, workers, workers, workers, long.MaxValue);

        Assert.Equal(ScalingReason.ProbeUp, result.Reason);
        Assert.Equal(int.MaxValue, result.DesiredWorkerCount);
        Assert.Equal(2, policy.ActiveProbeStep);
    }

    [Fact]
    public void ZeroProbeWindowCannotUseHistoricalEwmaAsPositiveThroughput()
    {
        var policy = CreatePolicy(warmupSamples: 0);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 0, ready: 0);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 0, ready: 0);
        Observe(policy, 2, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 3, 100, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 4, 100, workers: 2, desired: 2, busy: 2, ready: 10);
        var result = Observe(policy, 5, 100, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Null(result.ProbeGain);
        Assert.True(result.SmoothedThroughput > 0);
    }

    [Fact]
    public void BusyWorkersAtKeyLimitCanAcceptProbeWithoutStartingAnother()
    {
        var policy = CreatePolicy(measurementSamples: 2, warmupSamples: 0);
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 1);
        Observe(policy, 1, 100, workers: 1, desired: 1, busy: 1, ready: 1);
        var started = Observe(policy, 2, 200, workers: 1, desired: 1, busy: 1, ready: 1);
        Observe(policy, 3, 400, workers: 2, desired: 2, busy: 2, ready: 0);
        Observe(policy, 4, 600, workers: 2, desired: 2, busy: 2, ready: 0);
        var accepted = Observe(policy, 5, 800, workers: 2, desired: 2, busy: 2, ready: 0);
        var stable = Observe(policy, 6, 1000, workers: 2, desired: 2, busy: 2, ready: 0);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(1, accepted.ProbeGain!.Value, 10);
        Assert.False(accepted.IsSaturated);
        Assert.Equal(ScalingReason.None, stable.Reason);
        Assert.Equal(2, stable.DesiredWorkerCount);
    }

    [Fact]
    public void LargeCompletionCountersPreserveSingleMessageDeltas()
    {
        var policy = CreatePolicy(warmupSamples: 0);
        const long completed = 1L << 53;
        Observe(policy, 0, completed, workers: 1, desired: 1, busy: 1, ready: 10);
        var started = Observe(policy, 1, completed + 1, workers: 1, desired: 1, busy: 1, ready: 10);
        Observe(policy, 2, completed + 2, workers: 2, desired: 2, busy: 2, ready: 10);
        var result = Observe(policy, 3, completed + 3, workers: 2, desired: 2, busy: 2, ready: 10);

        Assert.Equal(1, started.Throughput);
        Assert.Equal(ScalingReason.ProbeRejected, result.Reason);
        Assert.Equal(0, result.ProbeGain!.Value, 10);
    }

    private static void AcceptStrongOneToTwoWorkerProbe(DynamicScalingPolicy policy)
    {
        Observe(policy, 0, 0, workers: 1, desired: 1, busy: 1, ready: 10);
        var started = Observe(
            policy,
            1,
            100,
            workers: 1,
            desired: 1,
            busy: 1,
            ready: 10);
        Observe(policy, 2, 200, workers: 2, desired: 2, busy: 2, ready: 10);
        var accepted = Observe(
            policy,
            3,
            400,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(2, started.DesiredWorkerCount);
        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(2, accepted.DesiredWorkerCount);
        Assert.Equal(2, policy.NextProbeStep);
    }

    private static void AcceptStrongTwoToFourWorkerProbe(DynamicScalingPolicy policy)
    {
        AcceptStrongOneToTwoWorkerProbe(policy);
        var started = Observe(
            policy,
            4,
            500,
            workers: 2,
            desired: 2,
            busy: 2,
            ready: 10);
        Observe(policy, 5, 600, workers: 4, desired: 4, busy: 4, ready: 10);
        var accepted = Observe(
            policy,
            6,
            800,
            workers: 4,
            desired: 4,
            busy: 4,
            ready: 10);

        Assert.Equal(ScalingReason.ProbeUp, started.Reason);
        Assert.Equal(4, started.DesiredWorkerCount);
        Assert.Equal(ScalingReason.ProbeAccepted, accepted.Reason);
        Assert.Equal(4, accepted.DesiredWorkerCount);
        Assert.Equal(4, policy.NextProbeStep);
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
        TimeSpan? scaleDownIdleDuration = null,
        int measurementSamples = 1,
        TimeSpan? probeTimeout = null)
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
                ThroughputMeasurementSamples = measurementSamples,
                ProbeTimeout = probeTimeout ?? TimeSpan.FromSeconds(10),
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
