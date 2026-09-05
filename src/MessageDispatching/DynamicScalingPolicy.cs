using System.Diagnostics;

namespace MessageDispatching;

internal enum DynamicScaleDecision
{
    None,
    ScaleUp,
    ScaleDown
}

internal sealed class DynamicScalingPolicy
{
    private const long NoTimestamp = long.MinValue;

    private readonly record struct TimedSample(
        long StartTimestamp,
        long EndTimestamp,
        double Utilization,
        double Saturation);

    private readonly DispatcherOptions _options;
    private readonly long _windowTimestampTicks;
    private readonly Queue<TimedSample> _samples;
    private double _weightedUtilization;
    private double _weightedSaturation;
    private long _coveredTimestampTicks;
    private int _nonZeroUtilizationSampleCount;
    private int _saturatedSampleCount;
    private long _trimmedHeadStartTimestamp = NoTimestamp;
    private long _lastSampleTimestamp = NoTimestamp;
    private long _lastScaleChangeTimestamp = NoTimestamp;
    private double _lastUtilization;
    private double _lastSaturation;

    public DynamicScalingPolicy(DispatcherOptions options)
    {
        _options = options;
        _windowTimestampTicks = checked((long)Math.Ceiling(
            options.ScaleObservationWindow.TotalSeconds * Stopwatch.Frequency));

        var expectedSamples = options.ScaleObservationWindow.Ticks / options.ScaleInterval.Ticks;
        if (options.ScaleObservationWindow.Ticks % options.ScaleInterval.Ticks != 0)
        {
            expectedSamples++;
        }

        // Start near the nominal sample count. Queue<T> uses an array-backed ring internally and
        // grows automatically when delayed PeriodicTimer continuations cluster observations.
        _samples = new Queue<TimedSample>(checked((int)expectedSamples + 1));
    }

    public DynamicScaleDecision Observe(
        int workerCount,
        int busyWorkers,
        int queuedWorkItems,
        bool retirementPending,
        long timestamp)
    {
        var normalizedWorkerCount = Math.Max(0, workerCount);
        var normalizedBusyWorkers = normalizedWorkerCount == 0
            ? 0
            : Math.Clamp(busyWorkers, 0, normalizedWorkerCount);
        var normalizedQueuedWorkItems = Math.Max(0, queuedWorkItems);
        var utilization = normalizedWorkerCount == 0
            ? 0d
            : (double)normalizedBusyWorkers / normalizedWorkerCount;
        var saturated = normalizedWorkerCount > 0 &&
            normalizedBusyWorkers == normalizedWorkerCount &&
            normalizedQueuedWorkItems > 0;

        if (_lastSampleTimestamp == NoTimestamp)
        {
            RecordLatestSample(timestamp, utilization, saturated);
            return DynamicScaleDecision.None;
        }

        if (timestamp <= _lastSampleTimestamp ||
            Stopwatch.GetElapsedTime(_lastSampleTimestamp, timestamp) >
            _options.ScaleObservationWindow)
        {
            ResetSamples();
            RecordLatestSample(timestamp, utilization, saturated);
            return DynamicScaleDecision.None;
        }

        TrimExpiredSamples(timestamp - _windowTimestampTicks);
        AddElapsedSample(timestamp);
        RecordLatestSample(timestamp, utilization, saturated);

        if (_coveredTimestampTicks < _windowTimestampTicks || retirementPending)
        {
            return DynamicScaleDecision.None;
        }

        var averageSaturation = _weightedSaturation / _coveredTimestampTicks;
        if (normalizedWorkerCount < _options.EffectiveMaxParallelism &&
            saturated &&
            averageSaturation >= _options.ScaleUpSaturationThreshold &&
            IsCooldownElapsed(timestamp, _options.ScaleUpCooldown))
        {
            return DynamicScaleDecision.ScaleUp;
        }

        var averageUtilization = _weightedUtilization / _coveredTimestampTicks;
        if (normalizedWorkerCount > _options.Parallelism &&
            normalizedBusyWorkers < normalizedWorkerCount &&
            normalizedQueuedWorkItems == 0 &&
            averageUtilization <= _options.ScaleDownUtilizationThreshold &&
            IsCooldownElapsed(timestamp, _options.ScaleDownCooldown))
        {
            return DynamicScaleDecision.ScaleDown;
        }

        return DynamicScaleDecision.None;
    }

    public void RecordScaleChange(long timestamp)
    {
        _lastScaleChangeTimestamp = timestamp;
    }

    private void AddElapsedSample(long timestamp)
    {
        var duration = timestamp - _lastSampleTimestamp;
        var sample = new TimedSample(
            _lastSampleTimestamp,
            timestamp,
            _lastUtilization,
            _lastSaturation);
        _samples.Enqueue(sample);
        _coveredTimestampTicks += duration;
        _weightedUtilization += duration * sample.Utilization;
        _weightedSaturation += duration * sample.Saturation;

        if (sample.Utilization > 0)
        {
            _nonZeroUtilizationSampleCount++;
        }

        if (sample.Saturation > 0)
        {
            _saturatedSampleCount++;
        }
    }

    private void TrimExpiredSamples(long cutoffTimestamp)
    {
        while (_samples.Count > 0)
        {
            var sample = _samples.Peek();
            var sampleStartTimestamp = _trimmedHeadStartTimestamp == NoTimestamp
                ? sample.StartTimestamp
                : _trimmedHeadStartTimestamp;

            if (sample.EndTimestamp <= cutoffTimestamp)
            {
                _samples.Dequeue();
                RemoveDuration(sample, sample.EndTimestamp - sampleStartTimestamp);
                _trimmedHeadStartTimestamp = NoTimestamp;

                if (sample.Utilization > 0)
                {
                    _nonZeroUtilizationSampleCount--;
                }

                if (sample.Saturation > 0)
                {
                    _saturatedSampleCount--;
                }

                continue;
            }

            if (sampleStartTimestamp < cutoffTimestamp)
            {
                var removedDuration = cutoffTimestamp - sampleStartTimestamp;
                RemoveDuration(sample, removedDuration);
                _trimmedHeadStartTimestamp = cutoffTimestamp;
            }

            break;
        }

        if (_nonZeroUtilizationSampleCount == 0)
        {
            _weightedUtilization = 0;
        }

        if (_saturatedSampleCount == 0)
        {
            _weightedSaturation = 0;
        }

        if (_samples.Count == 0)
        {
            _coveredTimestampTicks = 0;
            _trimmedHeadStartTimestamp = NoTimestamp;
        }
    }

    private void RemoveDuration(TimedSample sample, long duration)
    {
        _coveredTimestampTicks -= duration;
        _weightedUtilization -= duration * sample.Utilization;
        _weightedSaturation -= duration * sample.Saturation;
    }

    private void RecordLatestSample(long timestamp, double utilization, bool saturated)
    {
        _lastSampleTimestamp = timestamp;
        _lastUtilization = utilization;
        _lastSaturation = saturated ? 1d : 0d;
    }

    private bool IsCooldownElapsed(long timestamp, TimeSpan cooldown)
    {
        return _lastScaleChangeTimestamp == NoTimestamp ||
            Stopwatch.GetElapsedTime(_lastScaleChangeTimestamp, timestamp) >= cooldown;
    }

    private void ResetSamples()
    {
        _samples.Clear();
        _weightedUtilization = 0;
        _weightedSaturation = 0;
        _coveredTimestampTicks = 0;
        _nonZeroUtilizationSampleCount = 0;
        _saturatedSampleCount = 0;
        _trimmedHeadStartTimestamp = NoTimestamp;
        _lastSampleTimestamp = NoTimestamp;
        _lastUtilization = 0;
        _lastSaturation = 0;
    }
}
