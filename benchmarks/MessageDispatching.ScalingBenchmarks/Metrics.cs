using System.Diagnostics;
using System.Globalization;
using MessageDispatching;

namespace MessageDispatching.ScalingBenchmarks;

internal readonly record struct MetricSnapshot(
    long PendingMessages,
    long CompletedMessages,
    int KeyCount,
    int WorkerCount,
    int DesiredWorkerCount,
    int BusyWorkers,
    long ReadyOrQueuedCount,
    double Throughput,
    double SmoothedThroughput,
    bool IsSaturated,
    long ScaleUpCount,
    long ScaleDownCount,
    long ProbeAcceptCount,
    long ProbeRejectCount,
    double? LastProbeGain,
    bool Accepting)
{
    public static MetricSnapshot From(KeyedDispatcherStats stats)
    {
        return new MetricSnapshot(
            stats.PendingMessages,
            stats.CompletedMessages,
            stats.KeyCount,
            stats.WorkerCount,
            stats.DesiredWorkerCount,
            stats.BusyWorkers,
            stats.ReadyKeyCount,
            stats.Throughput,
            stats.SmoothedThroughput,
            stats.IsSaturated,
            stats.ScaleUpCount,
            stats.ScaleDownCount,
            stats.ProbeAcceptCount,
            stats.ProbeRejectCount,
            stats.LastProbeGain,
            stats.Accepting);
    }

    public static MetricSnapshot From(DispatcherStats stats)
    {
        return new MetricSnapshot(
            stats.PendingMessages,
            stats.CompletedMessages,
            0,
            stats.WorkerCount,
            stats.DesiredWorkerCount,
            stats.BusyWorkers,
            stats.QueuedMessageCount,
            stats.Throughput,
            stats.SmoothedThroughput,
            stats.IsSaturated,
            stats.ScaleUpCount,
            stats.ScaleDownCount,
            stats.ProbeAcceptCount,
            stats.ProbeRejectCount,
            stats.LastProbeGain,
            stats.Accepting);
    }
}

internal sealed class ScalingObservations
{
    private readonly int _maxParallelism;
    private int _peakWorkers;
    private long _workerBoundViolations;

    public ScalingObservations(int maxParallelism)
    {
        _maxParallelism = maxParallelism;
    }

    public int PeakWorkers => Volatile.Read(ref _peakWorkers);

    public long WorkerBoundViolations => Interlocked.Read(ref _workerBoundViolations);

    public void Observe(MetricSnapshot snapshot)
    {
        ObserveWorkerCount(snapshot.WorkerCount);
        ObserveWorkerCount(snapshot.DesiredWorkerCount);
    }

    public void Observe(DispatcherScaleChange change)
    {
        ObserveWorkerCount(change.PreviousWorkerCount);
        ObserveWorkerCount(change.CurrentWorkerCount);
        ObserveWorkerCount(change.Stats.WorkerCount);
        ObserveWorkerCount(change.Stats.DesiredWorkerCount);
    }

    private void ObserveWorkerCount(int value)
    {
        if (value > _maxParallelism)
        {
            Interlocked.Increment(ref _workerBoundViolations);
        }

        while (true)
        {
            var current = Volatile.Read(ref _peakWorkers);
            if (value <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _peakWorkers, value, current) == current)
            {
                return;
            }
        }
    }
}

internal readonly record struct ScenarioMetadata(
    string Name,
    string Dispatcher,
    string Workload);

internal readonly record struct RuntimeMeasurements(
    long Enqueued,
    long Processed,
    long Checksum,
    int PeakWorkers,
    int MaxProcessingConcurrency,
    long OrderingViolations,
    long SameKeyConcurrencyViolations,
    long WorkerBoundViolations);

internal sealed class ScenarioResult
{
    public required ScenarioMetadata Metadata { get; init; }

    public required TimeSpan Elapsed { get; init; }

    public required MetricSnapshot FinalStats { get; init; }

    public required RuntimeMeasurements Measurements { get; init; }

    public required string Status { get; init; }

    public bool Success => string.Equals(Status, "PASS", StringComparison.Ordinal);
}

internal static class CsvReporter
{
    private static readonly object OutputGate = new();

    private static readonly string[] Header =
    [
        "row_type",
        "scenario",
        "dispatcher",
        "workload",
        "elapsed_ms",
        "enqueued",
        "pending",
        "completed",
        "key_count",
        "actual_workers",
        "desired_workers",
        "busy_workers",
        "ready_or_queued",
        "raw_throughput_per_sec",
        "smoothed_throughput_per_sec",
        "saturated",
        "scale_up_count",
        "scale_down_count",
        "probe_accept_count",
        "probe_reject_count",
        "last_probe_gain",
        "accepting",
        "peak_workers",
        "max_processing_concurrency",
        "processed",
        "checksum",
        "ordering_violations",
        "same_key_concurrency_violations",
        "worker_bound_violations",
        "status"
    ];

    public static void WriteHeader()
    {
        WriteValues(Header);
    }

    public static void WriteSample(
        ScenarioMetadata metadata,
        TimeSpan elapsed,
        MetricSnapshot stats,
        RuntimeMeasurements measurements)
    {
        WriteRow("sample", metadata, elapsed, stats, measurements, string.Empty);
    }

    public static void WriteSummary(ScenarioResult result)
    {
        WriteRow(
            "summary",
            result.Metadata,
            result.Elapsed,
            result.FinalStats,
            result.Measurements,
            result.Status);
    }

    public static void WriteFailure(string scenario, Exception exception)
    {
        var metadata = new ScenarioMetadata(scenario, "unknown", "unknown");
        var status = $"ERROR:{exception.GetType().Name}:{exception.Message}";
        WriteRow("summary", metadata, TimeSpan.Zero, default, default, status);
    }

    private static void WriteRow(
        string rowType,
        ScenarioMetadata metadata,
        TimeSpan elapsed,
        MetricSnapshot stats,
        RuntimeMeasurements measurements,
        string status)
    {
        var invariant = CultureInfo.InvariantCulture;
        var values = new[]
        {
            rowType,
            metadata.Name,
            metadata.Dispatcher,
            metadata.Workload,
            elapsed.TotalMilliseconds.ToString("F0", invariant),
            measurements.Enqueued.ToString(invariant),
            stats.PendingMessages.ToString(invariant),
            stats.CompletedMessages.ToString(invariant),
            stats.KeyCount.ToString(invariant),
            stats.WorkerCount.ToString(invariant),
            stats.DesiredWorkerCount.ToString(invariant),
            stats.BusyWorkers.ToString(invariant),
            stats.ReadyOrQueuedCount.ToString(invariant),
            stats.Throughput.ToString("F3", invariant),
            stats.SmoothedThroughput.ToString("F3", invariant),
            stats.IsSaturated ? "true" : "false",
            stats.ScaleUpCount.ToString(invariant),
            stats.ScaleDownCount.ToString(invariant),
            stats.ProbeAcceptCount.ToString(invariant),
            stats.ProbeRejectCount.ToString(invariant),
            stats.LastProbeGain?.ToString("F6", invariant) ?? string.Empty,
            stats.Accepting ? "true" : "false",
            measurements.PeakWorkers.ToString(invariant),
            measurements.MaxProcessingConcurrency.ToString(invariant),
            measurements.Processed.ToString(invariant),
            measurements.Checksum.ToString(invariant),
            measurements.OrderingViolations.ToString(invariant),
            measurements.SameKeyConcurrencyViolations.ToString(invariant),
            measurements.WorkerBoundViolations.ToString(invariant),
            status
        };

        WriteValues(values);
    }

    private static void WriteValues(IReadOnlyList<string> values)
    {
        var escaped = new string[values.Count];
        for (var index = 0; index < values.Count; index++)
        {
            escaped[index] = Escape(values[index]);
        }

        lock (OutputGate)
        {
            Console.WriteLine(string.Join(',', escaped));
        }
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny([',', '"', '\r', '\n']) < 0)
        {
            return value;
        }

        return $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }
}

internal static class PeriodicMetrics
{
    public static async Task RunAsync(
        ScenarioMetadata metadata,
        Stopwatch stopwatch,
        TimeSpan interval,
        Func<MetricSnapshot> getStats,
        Func<RuntimeMeasurements> getMeasurements,
        ScalingObservations observations,
        CancellationToken cancellationToken)
    {
        WriteCurrentSample();

        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                WriteCurrentSample();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }

        void WriteCurrentSample()
        {
            var stats = getStats();
            observations.Observe(stats);
            CsvReporter.WriteSample(metadata, stopwatch.Elapsed, stats, getMeasurements());
        }
    }
}
