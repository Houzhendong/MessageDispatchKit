using System.Diagnostics;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class LoadMatchingScalingPolicyTests
{
    [Fact]
    public void BackloggedLongHandlersGrowWithoutAnyCompletions()
    {
        var policy = Create();
        Observe(policy, 0, workers: 1, busy: 1, active: 8, ready: 7);
        var result = Observe(policy, 0.1, workers: 1, busy: 1, active: 8, ready: 7);
        Assert.Equal(2, result.DesiredWorkerCount);
        Assert.Equal(ScalingReason.LoadScaleUp, result.Reason);
    }

    [Fact]
    public void QueueDelayExpandsEvenWhenSchedulingOverheadLowersOccupancy()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 3, active: 32, ready: 20);
        var result = Observe(policy, 0.1, workers: 8, busy: 3, active: 32, ready: 20, waitMs: 80);
        Assert.Equal(10, result.DesiredWorkerCount);
    }

    [Fact]
    public void SingleKeyCannotGrowEvenIfBusyAndReadyAreBothObserved()
    {
        var policy = Create();
        for (var i = 0; i < 100; i++)
        {
            var result = Observe(policy, i * 0.1, workers: 1, busy: 1, active: 1, ready: 1, waitMs: 100);
            Assert.Equal(1, result.DesiredWorkerCount);
        }
    }

    [Fact]
    public void ActiveKeyCountRatherThanHistoricalKeysBoundsGrowth()
    {
        var policy = Create();
        Observe(policy, 0, workers: 4, busy: 4, active: 5, ready: 100);
        var result = Observe(policy, 0.1, workers: 4, busy: 4, active: 5, ready: 100, waitMs: 100);
        Assert.Equal(5, result.DesiredWorkerCount);
    }

    [Fact]
    public void ShrinksUnderPartialLoadWithoutWaitingForEveryWorkerToBeIdle()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 3, active: 16, ready: 1);
        Observe(policy, 0.1, workers: 8, busy: 3, active: 16, ready: 1);
        var before = Observe(policy, 1, workers: 8, busy: 3, active: 16, ready: 1);
        var after = Observe(policy, 1.1, workers: 8, busy: 3, active: 16, ready: 1);
        Assert.Equal(8, before.DesiredWorkerCount);
        Assert.Equal(6, after.DesiredWorkerCount);
        Assert.Equal(ScalingReason.LoadScaleDown, after.Reason);
    }

    [Fact]
    public void ScaleDownKeepsHighestRecommendationInStabilizationWindow()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.1, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.6, workers: 8, busy: 5.5, active: 16);
        var result = Observe(policy, 1.1, workers: 8, busy: 2, active: 16);
        Assert.Equal(7, result.DesiredWorkerCount);
    }

    [Fact]
    public void ReturningDemandBreaksScaleDownStabilization()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.1, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.9, workers: 8, busy: 6.4, active: 16);
        var result = Observe(policy, 1.1, workers: 8, busy: 3, active: 16);
        Assert.Equal(8, result.DesiredWorkerCount);
    }

    [Fact]
    public void BusyTimeSmoothingUsesElapsedTime()
    {
        var policy = Create(window: TimeSpan.FromSeconds(1));
        Observe(policy, 0, workers: 8, busy: 0, active: 16);
        Observe(policy, 0.25, workers: 8, busy: 8, active: 16);
        Observe(policy, 1, workers: 8, busy: 8, active: 16);
        Assert.Equal(8 * (1 - Math.Exp(-1)), policy.AverageBusyWorkers, 8);
    }

    [Fact]
    public void CpuPressurePreventsGrowthButDoesNotForceActiveHandlersToStop()
    {
        var policy = Create();
        Observe(policy, 0, workers: 4, busy: 4, active: 16, ready: 12);
        var result = Observe(policy, 0.1, workers: 4, busy: 4, active: 16, ready: 12, cpu: 0.95);
        Assert.Equal(4, result.DesiredWorkerCount);
    }

    [Fact]
    public void ResourceBudgetAndMinimumAreRespected()
    {
        var policy = Create(resourceLimit: 4);
        Observe(policy, 0, workers: 4, busy: 4, active: 100, ready: 96);
        Assert.Equal(4, Observe(policy, 0.1, workers: 4, busy: 4, active: 100, ready: 96).DesiredWorkerCount);
        var floor = Create(minimum: 2, resourceLimit: 1);
        Observe(floor, 0, workers: 2, busy: 0, active: 0);
        Observe(floor, 0.1, workers: 2, busy: 0, active: 0);
        Assert.Equal(2, Observe(floor, 2, workers: 2, busy: 0, active: 0).DesiredWorkerCount);
    }

    [Fact]
    public void DoesNotIssueAnotherChangeWhileWorkersAreRetiring()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 8, active: 32, ready: 24);
        var result = Observe(policy, 1, workers: 8, busy: 8, active: 32, ready: 24, desired: 6, waitMs: 100);
        Assert.Equal(6, result.DesiredWorkerCount);
    }

    [Fact]
    public void SlowSamplingCanScaleWithoutAMeasurementDeadline()
    {
        var policy = Create();
        Observe(policy, 0, workers: 1, busy: 1, active: 32, ready: 31);
        Assert.Equal(2, Observe(policy, 20, workers: 1, busy: 1, active: 32, ready: 31).DesiredWorkerCount);
    }

    [Fact]
    public void CapacityFeedbackOnlyUsesSustainedIndependentBacklogAndRetriesSmallerStep()
    {
        var policy = Create(upInterval: TimeSpan.FromSeconds(10));
        // Hold growth with CPU pressure while collecting a comparable baseline at 4 workers.
        Observe(policy, 0, workers: 4, busy: 4, active: 16, ready: 12, completed: 0);
        Observe(policy, 1, workers: 4, busy: 4, active: 16, ready: 12, completed: 100, cpu: 1);
        Observe(policy, 2, workers: 4, busy: 4, active: 16, ready: 12, completed: 200, cpu: 1);
        var grow = Observe(policy, 3, workers: 4, busy: 4, active: 16, ready: 12, completed: 300);
        Assert.Equal(5, grow.DesiredWorkerCount);
        Observe(policy, 4, workers: 5, busy: 5, active: 16, ready: 11, completed: 400);
        Observe(policy, 5, workers: 5, busy: 5, active: 16, ready: 11, completed: 500);
        Observe(policy, 6, workers: 5, busy: 5, active: 16, ready: 11, completed: 600);
        Assert.Equal(4, policy.CapacityLimit);
        var rollback = Observe(policy, 7, workers: 5, busy: 5, active: 16, ready: 11, completed: 700);
        Assert.Equal(4, rollback.DesiredWorkerCount);
        Observe(policy, 16, workers: 4, busy: 4, active: 16, ready: 12, completed: 1600);
        Assert.Equal(5, policy.CapacityLimit);
    }

    [Fact]
    public void InputLimitedThroughputDoesNotLearnFalseCapacityCeiling()
    {
        var policy = Create(upInterval: TimeSpan.FromSeconds(10));
        Observe(policy, 0, workers: 4, busy: 4, active: 16, ready: 12);
        Observe(policy, 1, workers: 4, busy: 4, active: 16, ready: 12, completed: 100, cpu: 1);
        Observe(policy, 2, workers: 4, busy: 4, active: 16, ready: 12, completed: 200, cpu: 1);
        Observe(policy, 3, workers: 4, busy: 4, active: 16, ready: 12, completed: 300);
        for (var second = 4; second < 8; second++)
        {
            Observe(policy, second, workers: 5, busy: 3, active: 5, completed: second * 100);
        }
        Assert.Equal(32, policy.CapacityLimit);
    }

    [Fact]
    public void InvalidSampleResetsLowLoadWindow()
    {
        var policy = Create();
        Observe(policy, 0, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.1, workers: 8, busy: 3, active: 16);
        Observe(policy, 0.1, workers: 8, busy: 3, active: 16);
        var result = Observe(policy, 1.2, workers: 8, busy: 3, active: 16);
        Assert.Equal(8, result.DesiredWorkerCount);
    }

    [Fact]
    public void SmallPositiveCapacityGainIsKeptSoSingleWorkerRetriesCanReachTheOptimum()
    {
        var policy = new LoadMatchingScalingPolicy(new DispatcherOptions
        {
            Parallelism = 1, MaxParallelism = 128,
            DynamicScaling = new DynamicScalingOptions
            {
                ResourceParallelismLimit = 128,
                UtilizationWindow = TimeSpan.FromMilliseconds(1),
                CapacityMeasurementWindow = TimeSpan.FromSeconds(1),
                ScaleUpInterval = TimeSpan.FromSeconds(10)
            }
        });
        Observe(policy, 0, workers: 64, busy: 64, active: 100, ready: 36);
        Observe(policy, 1, workers: 64, busy: 64, active: 100, ready: 36, completed: 6400, cpu: 1);
        Observe(policy, 2, workers: 64, busy: 64, active: 100, ready: 36, completed: 12800, cpu: 1);
        Observe(policy, 3, workers: 64, busy: 64, active: 65, ready: 1, completed: 19200);
        // A one-worker target can result from the active-key or resource cap. Its 1.56%
        // gain is useful even though it is smaller than the legacy 2% probe threshold.
        Observe(policy, 4, workers: 65, busy: 65, active: 100, ready: 35, completed: 25700);
        Observe(policy, 5, workers: 65, busy: 65, active: 100, ready: 35, completed: 32200);
        var result = Observe(policy, 6, workers: 65, busy: 65, active: 100, ready: 35, completed: 38700);
        Assert.Equal(65, policy.CapacityLimit);
        Assert.Equal(65, result.DesiredWorkerCount);
    }

    [Fact]
    public void IndependentMessageCountsCanExceedInt32WithoutOverflowingTarget()
    {
        var policy = Create();
        var first = new LoadSnapshot(16, 16, long.MaxValue, long.MaxValue, 16,
            TimeSpan.FromSeconds(1), 0, 0, 0);
        policy.Observe(first);
        var result = policy.Observe(first with { Timestamp = Stopwatch.Frequency, CompletedMessages = 100 });
        Assert.Equal(20, result.DesiredWorkerCount);
    }

    private static LoadMatchingScalingPolicy Create(int minimum = 1, int resourceLimit = 32,
        TimeSpan? window = null, TimeSpan? upInterval = null)
        => new(new DispatcherOptions
        {
            Parallelism = minimum,
            MaxParallelism = 32,
            DynamicScaling = new DynamicScalingOptions
            {
                ResourceParallelismLimit = resourceLimit,
                UtilizationWindow = window ?? TimeSpan.FromMilliseconds(1),
                ScaleUpInterval = upInterval ?? TimeSpan.Zero,
                ScaleDownIdleDuration = TimeSpan.FromSeconds(1),
                CapacityMeasurementWindow = TimeSpan.FromSeconds(1),
                CapacityRetryInterval = TimeSpan.FromSeconds(10)
            }
        });

    private static ScalingResult Observe(LoadMatchingScalingPolicy policy, double seconds,
        int workers, double busy, int active, int ready = 0, double waitMs = 0,
        long completed = 0, double cpu = 0, int? desired = null)
        => policy.Observe(new LoadSnapshot(workers, desired ?? workers, active, ready, busy,
            TimeSpan.FromMilliseconds(waitMs), completed, cpu, (long)(seconds * Stopwatch.Frequency)));
}
