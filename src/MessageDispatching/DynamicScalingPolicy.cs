using System.Diagnostics;

namespace MessageDispatching;

internal readonly record struct ScalingSnapshot(
    int WorkerCount,
    int DesiredWorkerCount,
    int BusyWorkers,
    long RunnableWorkItemCount,
    long CompletedMessages,
    long Timestamp);

internal readonly record struct ScalingResult(
    int DesiredWorkerCount,
    ScalingReason Reason,
    double Throughput,
    double SmoothedThroughput,
    bool IsSaturated,
    double? ProbeGain);

internal enum ScalingReason
{
    None,
    ProbeUp,
    ProbeAccepted,
    ProbeRejected,
    IdleScaleDown
}

internal enum ScalingState
{
    Stable,
    ProbeConvergence,
    ProbeWarmup,
    ProbeMeasure,
    RollbackConvergence,
    ScaleDownConvergence
}

internal sealed class DynamicScalingPolicy
{
    private readonly int _minimumWorkerCount;
    private readonly int _maximumWorkerCount;
    private readonly double _minimumUsefulThroughputGain;
    private readonly double _throughputSmoothingFactor;
    private readonly int _probeWarmupSamples;
    private readonly TimeSpan _scaleUpCooldown;
    private readonly TimeSpan _scaleDownIdleDuration;

    private ScalingState _state;
    private bool _hasSampleBaseline;
    private long _lastCompletedMessages;
    private long _lastTimestamp;
    private bool _hasSmoothedThroughput;
    private double _smoothedThroughput;

    private int _baselineWorkerCount;
    private double _baselineThroughput;
    private int _nextProbeStep = 1;
    private int _activeProbeStep;
    private int _probeTargetWorkerCount;
    private int _warmupSamplesRemaining;

    private int _scaleDownTargetWorkerCount;
    private bool _hasIdleStart;
    private long _idleStartTimestamp;
    private bool _hasCooldownStart;
    private long _cooldownStartTimestamp;

    public DynamicScalingPolicy(DispatcherOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.DynamicScaling);

        _minimumWorkerCount = options.Parallelism;
        _maximumWorkerCount = options.EffectiveMaxParallelism;
        _minimumUsefulThroughputGain = options.DynamicScaling.MinimumUsefulThroughputGain;
        _throughputSmoothingFactor = options.DynamicScaling.ThroughputSmoothingFactor;
        _probeWarmupSamples = options.DynamicScaling.ProbeWarmupSamples;
        _scaleUpCooldown = options.DynamicScaling.ScaleUpCooldown;
        _scaleDownIdleDuration = options.DynamicScaling.ScaleDownIdleDuration;
    }

    internal ScalingState State => _state;

    internal int NextProbeStep => _nextProbeStep;

    internal int ActiveProbeStep => _activeProbeStep;

    public ScalingResult Observe(ScalingSnapshot snapshot)
    {
        var isSaturated = IsSaturated(snapshot);

        if (!_hasSampleBaseline)
        {
            RebaselineSample(snapshot);
            StartIdlePeriodIfEligible(snapshot);
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                0,
                isSaturated,
                null);
        }

        if (snapshot.Timestamp <= _lastTimestamp ||
            snapshot.CompletedMessages < _lastCompletedMessages)
        {
            RebaselineSample(snapshot);
            ResetIdlePeriod();

            if (IsProbeActive())
            {
                return RejectProbe(0, isSaturated, null);
            }

            return CreateResult(
                GetDesiredWorkerCount(snapshot),
                ScalingReason.None,
                0,
                isSaturated,
                null);
        }

        var elapsedSeconds = Stopwatch.GetElapsedTime(_lastTimestamp, snapshot.Timestamp).TotalSeconds;
        var completedDelta = (double)snapshot.CompletedMessages - _lastCompletedMessages;
        var throughput = completedDelta / elapsedSeconds;

        _lastTimestamp = snapshot.Timestamp;
        _lastCompletedMessages = snapshot.CompletedMessages;
        UpdateSmoothedThroughput(throughput);

        return _state switch
        {
            ScalingState.Stable => ObserveStable(snapshot, throughput, isSaturated),
            ScalingState.ProbeConvergence => ObserveProbeConvergence(snapshot, throughput, isSaturated),
            ScalingState.ProbeWarmup => ObserveProbeWarmup(snapshot, throughput, isSaturated),
            ScalingState.ProbeMeasure => ObserveProbeMeasure(snapshot, throughput, isSaturated),
            ScalingState.RollbackConvergence => ObserveRollbackConvergence(
                snapshot,
                throughput,
                isSaturated),
            ScalingState.ScaleDownConvergence => ObserveScaleDownConvergence(
                snapshot,
                throughput,
                isSaturated),
            _ => throw new InvalidOperationException($"Unknown scaling state: {_state}.")
        };
    }

    private ScalingResult ObserveStable(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (snapshot.WorkerCount != snapshot.DesiredWorkerCount)
        {
            ResetIdlePeriod();
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        if (IsIdleScaleDownEligible(snapshot))
        {
            if (!_hasIdleStart)
            {
                _hasIdleStart = true;
                _idleStartTimestamp = snapshot.Timestamp;
            }
            else if (HasElapsed(_idleStartTimestamp, snapshot.Timestamp, _scaleDownIdleDuration))
            {
                _scaleDownTargetWorkerCount = snapshot.DesiredWorkerCount - 1;
                _nextProbeStep = 1;
                _state = ScalingState.ScaleDownConvergence;
                ResetIdlePeriod();

                return CreateResult(
                    _scaleDownTargetWorkerCount,
                    ScalingReason.IdleScaleDown,
                    throughput,
                    isSaturated,
                    null);
            }
        }
        else
        {
            ResetIdlePeriod();
        }

        if (!isSaturated || !_hasSmoothedThroughput)
        {
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        if (!IsScaleUpCooldownElapsed(snapshot.Timestamp))
        {
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        _hasCooldownStart = false;

        var runnableParallelism = GetRunnableParallelism(snapshot);
        if (snapshot.WorkerCount >= _maximumWorkerCount ||
            runnableParallelism <= snapshot.WorkerCount)
        {
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        _baselineWorkerCount = snapshot.WorkerCount;
        _baselineThroughput = _smoothedThroughput;

        var requestedStep = _baselineThroughput > 0
            ? _nextProbeStep
            : 1;
        var proportionalStepCap = snapshot.WorkerCount <= 2
            ? snapshot.WorkerCount
            : (snapshot.WorkerCount / 2) + (snapshot.WorkerCount % 2);
        var actualStep = Math.Min((long)requestedStep, proportionalStepCap);
        actualStep = Math.Min(
            actualStep,
            (long)_maximumWorkerCount - snapshot.WorkerCount);
        actualStep = Math.Min(
            actualStep,
            runnableParallelism - snapshot.WorkerCount);

        if (actualStep <= 0)
        {
            return CreateResult(
                snapshot.DesiredWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        _activeProbeStep = (int)actualStep;
        _probeTargetWorkerCount = snapshot.WorkerCount + _activeProbeStep;
        _warmupSamplesRemaining = _probeWarmupSamples;
        _state = ScalingState.ProbeConvergence;
        ResetIdlePeriod();

        return CreateResult(
            _probeTargetWorkerCount,
            ScalingReason.ProbeUp,
            throughput,
            isSaturated,
            null);
    }

    private ScalingResult ObserveProbeConvergence(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (HasConverged(snapshot, _probeTargetWorkerCount))
        {
            _warmupSamplesRemaining = _probeWarmupSamples;
            _state = _warmupSamplesRemaining == 0
                ? ScalingState.ProbeMeasure
                : ScalingState.ProbeWarmup;
        }

        return CreateResult(
            _probeTargetWorkerCount,
            ScalingReason.None,
            throughput,
            isSaturated,
            null);
    }

    private ScalingResult ObserveProbeWarmup(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (!HasConverged(snapshot, _probeTargetWorkerCount))
        {
            _warmupSamplesRemaining = _probeWarmupSamples;
            _state = ScalingState.ProbeConvergence;
        }
        else if (--_warmupSamplesRemaining == 0)
        {
            _state = ScalingState.ProbeMeasure;
        }

        return CreateResult(
            _probeTargetWorkerCount,
            ScalingReason.None,
            throughput,
            isSaturated,
            null);
    }

    private ScalingResult ObserveProbeMeasure(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (!HasConverged(snapshot, _probeTargetWorkerCount))
        {
            _warmupSamplesRemaining = _probeWarmupSamples;
            _state = ScalingState.ProbeConvergence;

            return CreateResult(
                _probeTargetWorkerCount,
                ScalingReason.None,
                throughput,
                isSaturated,
                null);
        }

        if (GetRunnableParallelism(snapshot) < _probeTargetWorkerCount)
        {
            return RejectProbe(throughput, isSaturated, null);
        }

        if (_baselineThroughput <= 0)
        {
            if (_smoothedThroughput > 0)
            {
                return AcceptProbe(throughput, isSaturated, null);
            }

            return RejectProbe(throughput, isSaturated, null);
        }

        var gain = (_smoothedThroughput - _baselineThroughput) / _baselineThroughput;
        return gain >= _minimumUsefulThroughputGain
            ? AcceptProbe(throughput, isSaturated, gain)
            : RejectProbe(throughput, isSaturated, gain);
    }

    private ScalingResult ObserveRollbackConvergence(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (HasConverged(snapshot, _baselineWorkerCount))
        {
            _state = ScalingState.Stable;
            _hasCooldownStart = true;
            _cooldownStartTimestamp = snapshot.Timestamp;
            ResetIdlePeriod();
            StartIdlePeriodIfEligible(snapshot);
        }

        return CreateResult(
            _baselineWorkerCount,
            ScalingReason.None,
            throughput,
            isSaturated,
            null);
    }

    private ScalingResult ObserveScaleDownConvergence(
        ScalingSnapshot snapshot,
        double throughput,
        bool isSaturated)
    {
        if (HasConverged(snapshot, _scaleDownTargetWorkerCount))
        {
            _state = ScalingState.Stable;
            ResetIdlePeriod();
            StartIdlePeriodIfEligible(snapshot);
        }

        return CreateResult(
            _scaleDownTargetWorkerCount,
            ScalingReason.None,
            throughput,
            isSaturated,
            null);
    }

    private ScalingResult AcceptProbe(
        double throughput,
        bool isSaturated,
        double? gain)
    {
        if (_baselineThroughput > 0 && gain.HasValue)
        {
            var relativeWorkerIncrease =
                _activeProbeStep / (double)_baselineWorkerCount;
            var elasticity = gain.Value / relativeWorkerIncrease;

            if (elasticity >= 0.75)
            {
                _nextProbeStep = _activeProbeStep > int.MaxValue / 2
                    ? int.MaxValue
                    : _activeProbeStep * 2;
            }
            else if (elasticity >= 0.25)
            {
                _nextProbeStep = _activeProbeStep;
            }
            else
            {
                _nextProbeStep = Math.Max(1, _activeProbeStep / 2);
            }
        }
        else
        {
            _nextProbeStep = 1;
        }

        _baselineWorkerCount = _probeTargetWorkerCount;
        _baselineThroughput = _smoothedThroughput;
        _state = ScalingState.Stable;
        ResetIdlePeriod();

        return CreateResult(
            _probeTargetWorkerCount,
            ScalingReason.ProbeAccepted,
            throughput,
            isSaturated,
            gain);
    }

    private ScalingResult RejectProbe(
        double throughput,
        bool isSaturated,
        double? gain)
    {
        _nextProbeStep = gain.HasValue &&
            double.IsFinite(gain.Value) &&
            gain.Value >= 0
                ? Math.Max(1, _activeProbeStep / 2)
                : 1;
        _state = ScalingState.RollbackConvergence;
        ResetIdlePeriod();

        return CreateResult(
            _baselineWorkerCount,
            ScalingReason.ProbeRejected,
            throughput,
            isSaturated,
            gain);
    }

    private ScalingResult CreateResult(
        int desiredWorkerCount,
        ScalingReason reason,
        double throughput,
        bool isSaturated,
        double? probeGain)
    {
        return new ScalingResult(
            desiredWorkerCount,
            reason,
            throughput,
            _hasSmoothedThroughput ? _smoothedThroughput : 0,
            isSaturated,
            probeGain);
    }

    private void RebaselineSample(ScalingSnapshot snapshot)
    {
        _hasSampleBaseline = true;
        _lastCompletedMessages = snapshot.CompletedMessages;
        _lastTimestamp = snapshot.Timestamp;
    }

    private void UpdateSmoothedThroughput(double throughput)
    {
        if (!_hasSmoothedThroughput)
        {
            _smoothedThroughput = throughput;
            _hasSmoothedThroughput = true;
            return;
        }

        _smoothedThroughput =
            (_throughputSmoothingFactor * throughput) +
            ((1 - _throughputSmoothingFactor) * _smoothedThroughput);
    }

    private int GetDesiredWorkerCount(ScalingSnapshot snapshot)
    {
        return _state switch
        {
            ScalingState.ProbeConvergence or
            ScalingState.ProbeWarmup or
            ScalingState.ProbeMeasure => _probeTargetWorkerCount,
            ScalingState.RollbackConvergence => _baselineWorkerCount,
            ScalingState.ScaleDownConvergence => _scaleDownTargetWorkerCount,
            _ => snapshot.DesiredWorkerCount
        };
    }

    private bool IsProbeActive()
    {
        return _state is ScalingState.ProbeConvergence or
            ScalingState.ProbeWarmup or
            ScalingState.ProbeMeasure;
    }

    private bool IsScaleUpCooldownElapsed(long timestamp)
    {
        return !_hasCooldownStart ||
            HasElapsed(_cooldownStartTimestamp, timestamp, _scaleUpCooldown);
    }

    private bool IsIdleScaleDownEligible(ScalingSnapshot snapshot)
    {
        return snapshot.RunnableWorkItemCount == 0 &&
            snapshot.BusyWorkers < snapshot.DesiredWorkerCount &&
            snapshot.WorkerCount == snapshot.DesiredWorkerCount &&
            snapshot.DesiredWorkerCount > _minimumWorkerCount;
    }

    private void StartIdlePeriodIfEligible(ScalingSnapshot snapshot)
    {
        if (IsIdleScaleDownEligible(snapshot))
        {
            _hasIdleStart = true;
            _idleStartTimestamp = snapshot.Timestamp;
        }
    }

    private void ResetIdlePeriod()
    {
        _hasIdleStart = false;
    }

    private static bool HasConverged(ScalingSnapshot snapshot, int targetWorkerCount)
    {
        return snapshot.WorkerCount == targetWorkerCount &&
            snapshot.DesiredWorkerCount == targetWorkerCount;
    }

    private static bool IsSaturated(ScalingSnapshot snapshot)
    {
        return snapshot.WorkerCount > 0 &&
            snapshot.BusyWorkers >= snapshot.WorkerCount &&
            snapshot.RunnableWorkItemCount > 0;
    }

    private static long GetRunnableParallelism(ScalingSnapshot snapshot)
    {
        if (snapshot.BusyWorkers > 0 &&
            snapshot.RunnableWorkItemCount > long.MaxValue - snapshot.BusyWorkers)
        {
            return long.MaxValue;
        }

        if (snapshot.BusyWorkers < 0 &&
            snapshot.RunnableWorkItemCount < long.MinValue - snapshot.BusyWorkers)
        {
            return long.MinValue;
        }

        return snapshot.RunnableWorkItemCount + snapshot.BusyWorkers;
    }

    private static bool HasElapsed(long startTimestamp, long timestamp, TimeSpan duration)
    {
        return timestamp >= startTimestamp &&
            Stopwatch.GetElapsedTime(startTimestamp, timestamp) >= duration;
    }
}
