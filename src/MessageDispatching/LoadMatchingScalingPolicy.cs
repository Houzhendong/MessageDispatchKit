using System.Diagnostics;

namespace MessageDispatching;

internal readonly record struct LoadSnapshot(
    int WorkerCount,
    int DesiredWorkerCount,
    int ActiveKeyCount,
    int ReadyKeyCount,
    double AverageBusyWorkers,
    TimeSpan ReadyKeyWait,
    long CompletedMessages,
    double CpuUtilization,
    long Timestamp);

// Occupancy estimates demand, ready-key delay catches bursts, and capacity feedback
// bounds growth. Throughput is only compared while independent work remains queued.
internal sealed class LoadMatchingScalingPolicy
{
    private readonly int _minimum;
    private readonly int _maximum;
    private readonly DynamicScalingOptions _options;
    private bool _initialized;
    private long _lastTimestamp;
    private long _lastCompleted;
    private double _averageBusy;
    private double _smoothedThroughput;
    private bool _hasThroughput;
    private long? _lastScaleUp;
    private long? _lowLoadStart;
    private int _lowLoadHighWater;
    private int _capacityLimit;
    private long? _capacityLimitedAt;

    private long? _windowStart;
    private long _windowCompleted;
    private int _windowWorkers;
    private double? _referenceThroughput;
    private int _referenceWorkers;
    private bool _warmup;

    public LoadMatchingScalingPolicy(DispatcherOptions options)
    {
        _minimum = options.Parallelism;
        _maximum = Math.Min(options.EffectiveMaxParallelism,
            Math.Max(_minimum, options.DynamicScaling.ResourceParallelismLimit == 0
                ? Environment.ProcessorCount
                : options.DynamicScaling.ResourceParallelismLimit));
        _capacityLimit = _maximum;
        _options = options.DynamicScaling;
    }

    internal double AverageBusyWorkers => _averageBusy;
    internal int CapacityLimit => _capacityLimit;

    public ScalingResult Observe(LoadSnapshot snapshot)
    {
        var saturated = snapshot.WorkerCount > 0 &&
            snapshot.AverageBusyWorkers >= snapshot.WorkerCount * _options.TargetUtilization &&
            snapshot.ReadyKeyCount > 0;

        if (!_initialized || snapshot.Timestamp <= _lastTimestamp ||
            snapshot.CompletedMessages < _lastCompleted)
        {
            _initialized = true;
            _lastTimestamp = snapshot.Timestamp;
            _lastCompleted = snapshot.CompletedMessages;
            _averageBusy = Math.Clamp(snapshot.AverageBusyWorkers, 0, snapshot.WorkerCount);
            _lowLoadStart = null;
            ResetCapacityWindow();
            return Result(snapshot.DesiredWorkerCount, ScalingReason.None, 0, saturated);
        }

        var elapsed = Stopwatch.GetElapsedTime(_lastTimestamp, snapshot.Timestamp).TotalSeconds;
        var throughput = (snapshot.CompletedMessages - _lastCompleted) / elapsed;
        _lastTimestamp = snapshot.Timestamp;
        _lastCompleted = snapshot.CompletedMessages;
        var alpha = 1 - Math.Exp(-elapsed / _options.UtilizationWindow.TotalSeconds);
        _averageBusy += alpha * (Math.Clamp(snapshot.AverageBusyWorkers, 0, snapshot.WorkerCount) - _averageBusy);
        _smoothedThroughput = _hasThroughput
            ? _options.ThroughputSmoothingFactor * throughput +
                (1 - _options.ThroughputSmoothingFactor) * _smoothedThroughput
            : throughput;
        _hasThroughput = true;

        if (_capacityLimitedAt.HasValue &&
            Elapsed(_capacityLimitedAt.Value, snapshot.Timestamp, _options.CapacityRetryInterval))
        {
            // Forget a learned limit gradually, rather than jumping back to a failed large target.
            _capacityLimit = Math.Min(_maximum, _capacityLimit + 1);
            _capacityLimitedAt = _capacityLimit < _maximum ? snapshot.Timestamp : null;
        }

        ObserveCapacity(snapshot);
        if (snapshot.WorkerCount != snapshot.DesiredWorkerCount)
        {
            _lowLoadStart = null;
            return Result(snapshot.DesiredWorkerCount, ScalingReason.None, throughput, saturated);
        }

        var demand = (int)Math.Min(int.MaxValue,
            Math.Ceiling(_averageBusy / _options.TargetUtilization));
        if (snapshot.ReadyKeyCount > 0 && snapshot.ReadyKeyWait >= _options.TargetQueueWait)
        {
            demand = Math.Max(demand, AddStep(snapshot.WorkerCount));
        }

        var desired = Math.Clamp(Math.Min(demand,
            Math.Min(snapshot.ActiveKeyCount, _capacityLimit)), _minimum, _maximum);

        if (desired > snapshot.WorkerCount)
        {
            _lowLoadStart = null;
            if (snapshot.CpuUtilization >= _options.CpuUtilizationLimit ||
                (_lastScaleUp.HasValue &&
                 !Elapsed(_lastScaleUp.Value, snapshot.Timestamp, _options.ScaleUpInterval)))
            {
                return Result(snapshot.WorkerCount, ScalingReason.None, throughput, saturated);
            }

            desired = Math.Min(desired, AddStep(snapshot.WorkerCount));
            _lastScaleUp = snapshot.Timestamp;
            // The last full, backlogged window is a reference for opportunistic capacity checks.
            _warmup = true;
            _windowStart = null;
            return Result(desired, ScalingReason.LoadScaleUp, throughput, saturated);
        }

        if (desired < snapshot.WorkerCount)
        {
            if (!_lowLoadStart.HasValue)
            {
                _lowLoadStart = snapshot.Timestamp;
                _lowLoadHighWater = desired;
            }
            else
            {
                _lowLoadHighWater = Math.Max(_lowLoadHighWater, desired);
            }

            if (Elapsed(_lowLoadStart.Value, snapshot.Timestamp, _options.ScaleDownIdleDuration))
            {
                // Keep the highest recommendation in this stabilization window.
                desired = Math.Max(_lowLoadHighWater,
                    snapshot.WorkerCount - Math.Max(1, snapshot.WorkerCount / 4));
                _lowLoadStart = null;
                ResetCapacityWindow();
                return Result(desired, _capacityLimit < snapshot.WorkerCount
                        ? ScalingReason.CapacityLimited : ScalingReason.LoadScaleDown,
                    throughput, saturated);
            }
        }
        else
        {
            _lowLoadStart = null;
        }

        return Result(snapshot.WorkerCount, ScalingReason.None, throughput, saturated);
    }

    private bool ObserveCapacity(LoadSnapshot snapshot)
    {
        // Input-limited and key-limited samples cannot establish a throughput ceiling.
        if (snapshot.WorkerCount != snapshot.DesiredWorkerCount)
        {
            // Starting tasks is not an input interruption. Keep the reference, but exclude
            // the convergence interval from the new capacity measurement.
            _windowStart = null;
            return false;
        }

        if (snapshot.ReadyKeyCount == 0 || snapshot.ActiveKeyCount <= snapshot.WorkerCount ||
            snapshot.AverageBusyWorkers < snapshot.WorkerCount * _options.TargetUtilization)
        {
            ResetCapacityWindow();
            return false;
        }

        if (!_windowStart.HasValue || _windowWorkers != snapshot.WorkerCount)
        {
            _windowStart = snapshot.Timestamp;
            _windowCompleted = snapshot.CompletedMessages;
            _windowWorkers = snapshot.WorkerCount;
            return false;
        }

        if (!Elapsed(_windowStart.Value, snapshot.Timestamp, _options.CapacityMeasurementWindow))
        {
            return false;
        }

        var rate = (snapshot.CompletedMessages - _windowCompleted) /
            Stopwatch.GetElapsedTime(_windowStart.Value, snapshot.Timestamp).TotalSeconds;
        _windowStart = snapshot.Timestamp;
        _windowCompleted = snapshot.CompletedMessages;
        if (_warmup)
        {
            _warmup = false;
            return false;
        }

        var limited = _referenceThroughput is > 0 && snapshot.WorkerCount > _referenceWorkers &&
            rate <= _referenceThroughput.Value * (1 + _options.MinimumUsefulThroughputGain);
        if (limited)
        {
            // A small positive gain still helps load matching. Keep it and pause further
            // growth; rolling it back would reject every +1 retry at high parallelism.
            _capacityLimit = rate > _referenceThroughput!.Value
                ? snapshot.WorkerCount
                : Math.Max(_minimum, _referenceWorkers);
            _capacityLimitedAt = snapshot.Timestamp;
            ResetCapacityWindow();
            return true;
        }

        _referenceThroughput = rate;
        _referenceWorkers = snapshot.WorkerCount;
        return false;
    }

    private void ResetCapacityWindow()
    {
        _windowStart = null;
        _referenceThroughput = null;
        _warmup = false;
    }

    private ScalingResult Result(int desired, ScalingReason reason, double throughput, bool saturated)
        => new(desired, reason, throughput, _smoothedThroughput, saturated, null);

    private static int AddStep(int workers)
        => (int)Math.Min(int.MaxValue, (long)workers + Math.Max(1, workers / 4));

    private static bool Elapsed(long start, long end, TimeSpan duration)
        => Stopwatch.GetElapsedTime(start, end) >= duration;
}
