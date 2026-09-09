using System.Diagnostics;
using MessageDispatching;

namespace MessageDispatching.ScalingBenchmarks;

internal static class ScenarioCatalog
{
    private static readonly (string Name, string Description)[] Scenarios =
    [
        ("keyed-homogeneous", "Keyed homogeneous 32KB CPU work across 64 keys."),
        ("keyed-heterogeneous", "Keyed mixed 1KB/10KB/100KB/1MB CPU work."),
        ("keyed-1000-keys", "Keyed homogeneous CPU work across exactly 1000 keys."),
        ("keyed-one-hot-key", "A single hot ordered key with sustained CPU work."),
        ("keyed-four-hot-keys", "Four hot ordered keys with MaxParallelism fixed at 32."),
        ("keyed-low-to-high-burst", "A low-rate phase followed by a saturated keyed burst."),
        ("message-homogeneous", "Ordinary MessageDispatcher homogeneous CPU work.")
    ];

    private static readonly Dictionary<string, string> CanonicalNames = Scenarios
        .ToDictionary(static item => item.Name, static item => item.Name, StringComparer.OrdinalIgnoreCase);

    public static IReadOnlyList<string> Resolve(IReadOnlyList<string> requested)
    {
        if (requested.Count == 0 || requested.Any(static name =>
                string.Equals(name, "all", StringComparison.OrdinalIgnoreCase)))
        {
            return Scenarios.Select(static item => item.Name).ToArray();
        }

        var resolved = new List<string>(requested.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in requested)
        {
            if (!CanonicalNames.TryGetValue(name, out var canonicalName))
            {
                throw new ArgumentException(
                    $"Unknown scenario '{name}'. Use --list to see valid scenario names.");
            }

            if (seen.Add(canonicalName))
            {
                resolved.Add(canonicalName);
            }
        }

        return resolved;
    }

    public static void WriteList(TextWriter writer)
    {
        foreach (var (name, description) in Scenarios)
        {
            writer.WriteLine($"{name}\t{description}");
        }
    }

    private static void AddScenarioValues(List<string> scenarios, string value)
    {
        foreach (var item in value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            scenarios.Add(item);
        }
    }
}

internal static class ScenarioRunner
{
    private static readonly CpuWorkload HomogeneousWork =
        CpuWork.Create("32KBx4", 32 * 1024, 4, 0x13579BDFu);

    private static readonly CpuWorkload ManyKeysWork =
        CpuWork.Create("8KBx4", 8 * 1024, 4, 0x2468ACE1u);

    private static readonly CpuWorkload BurstWork =
        CpuWork.Create("16KBx4", 16 * 1024, 4, 0xDEADBEEFu);

    private static readonly CpuWorkload[] HeterogeneousWork =
    [
        CpuWork.Create("1KB", 1024, 2, 0xA341316Cu),
        CpuWork.Create("10KB", 10 * 1024, 2, 0xC8013EA4u),
        CpuWork.Create("100KB", 100 * 1024, 2, 0xAD90777Du),
        CpuWork.Create("1MB", 1024 * 1024, 2, 0x7E95761Eu)
    ];

    public static Task<ScenarioResult> RunAsync(string scenario, BenchmarkOptions options)
    {
        return scenario switch
        {
            "keyed-homogeneous" => RunKeyedSustainedAsync(
                new ScenarioMetadata(scenario, "KeyedOrderedDispatcher", HomogeneousWork.Name),
                options,
                keyCount: 64,
                workloads: [HomogeneousWork],
                targetPending: 1024,
                maxParallelism: options.MaxParallelism),

            "keyed-heterogeneous" => RunKeyedSustainedAsync(
                new ScenarioMetadata(
                    scenario,
                    "KeyedOrderedDispatcher",
                    "1KB|10KB|100KB|1MB"),
                options,
                keyCount: 64,
                workloads: HeterogeneousWork,
                targetPending: 128,
                maxParallelism: options.MaxParallelism),

            "keyed-1000-keys" => RunKeyedSustainedAsync(
                new ScenarioMetadata(scenario, "KeyedOrderedDispatcher", ManyKeysWork.Name),
                options,
                keyCount: 1000,
                workloads: [ManyKeysWork],
                targetPending: 2000,
                maxParallelism: options.MaxParallelism),

            "keyed-one-hot-key" => RunKeyedSustainedAsync(
                new ScenarioMetadata(scenario, "KeyedOrderedDispatcher", HomogeneousWork.Name),
                options,
                keyCount: 1,
                workloads: [HomogeneousWork],
                targetPending: 256,
                maxParallelism: options.MaxParallelism),

            "keyed-four-hot-keys" => RunKeyedSustainedAsync(
                new ScenarioMetadata(scenario, "KeyedOrderedDispatcher", HomogeneousWork.Name),
                options,
                keyCount: 4,
                workloads: [HomogeneousWork],
                targetPending: 512,
                maxParallelism: 32),

            "keyed-low-to-high-burst" => RunKeyedBurstAsync(
                new ScenarioMetadata(scenario, "KeyedOrderedDispatcher", BurstWork.Name),
                options,
                keyCount: 128,
                workloads: [BurstWork],
                highTargetPending: 1024,
                maxParallelism: options.MaxParallelism),

            "message-homogeneous" => RunMessageSustainedAsync(
                new ScenarioMetadata(scenario, "MessageDispatcher", HomogeneousWork.Name),
                options,
                workloads: [HomogeneousWork],
                targetPending: 1024,
                maxParallelism: options.MaxParallelism),

            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, "Unknown scenario.")
        };
    }

    private static Task<ScenarioResult> RunKeyedSustainedAsync(
        ScenarioMetadata metadata,
        BenchmarkOptions benchmarkOptions,
        int keyCount,
        CpuWorkload[] workloads,
        int targetPending,
        int maxParallelism)
    {
        return RunKeyedAsync(
            metadata,
            benchmarkOptions,
            keyCount,
            workloads,
            maxParallelism,
            (dispatcher, enqueueNext, stopwatch) => ProduceToBacklogAsync(
                () => dispatcher.GetStats().PendingMessages,
                enqueueNext,
                targetPending,
                stopwatch,
                benchmarkOptions.Duration));
    }

    private static Task<ScenarioResult> RunKeyedBurstAsync(
        ScenarioMetadata metadata,
        BenchmarkOptions benchmarkOptions,
        int keyCount,
        CpuWorkload[] workloads,
        int highTargetPending,
        int maxParallelism)
    {
        return RunKeyedAsync(
            metadata,
            benchmarkOptions,
            keyCount,
            workloads,
            maxParallelism,
            async (dispatcher, enqueueNext, stopwatch) =>
            {
                var lowPhaseEnd = TimeSpan.FromTicks(
                    checked((long)(benchmarkOptions.Duration.Ticks * 0.35)));

                while (stopwatch.Elapsed < lowPhaseEnd)
                {
                    enqueueNext();
                    await Task.Delay(TimeSpan.FromMilliseconds(8)).ConfigureAwait(false);
                }

                await ProduceToBacklogAsync(
                    () => dispatcher.GetStats().PendingMessages,
                    enqueueNext,
                    highTargetPending,
                    stopwatch,
                    benchmarkOptions.Duration).ConfigureAwait(false);
            });
    }

    private static async Task<ScenarioResult> RunKeyedAsync(
        ScenarioMetadata metadata,
        BenchmarkOptions benchmarkOptions,
        int keyCount,
        CpuWorkload[] workloads,
        int maxParallelism,
        Func<
            KeyedOrderedDispatcher<int, KeyedCpuMessage>,
            Action,
            Stopwatch,
            Task> produceAsync)
    {
        var observations = new ScalingObservations(maxParallelism);
        var handler = new OrderedCpuHandler(workloads);
        var counters = new RunCounters();
        var dispatcherOptions = CreateDispatcherOptions(
            benchmarkOptions,
            maxParallelism,
            observations);

        await using var dispatcher =
            new KeyedOrderedDispatcher<int, KeyedCpuMessage>(dispatcherOptions);
        dispatcher.Start(handler);

        var sequences = new int[keyCount];
        var nextKey = 0;
        var nextId = 0L;

        void EnqueueNext()
        {
            var key = nextKey;
            nextKey = (nextKey + 1) % keyCount;
            var sequence = sequences[key]++;
            var id = nextId++;
            var workloadIndex = (int)(id % workloads.Length);
            var salt = unchecked((key + 1) * 397 ^ sequence * 7919 ^ workloadIndex * 104729);

            dispatcher.Enqueue(
                key,
                new KeyedCpuMessage(sequence, workloadIndex, salt));
            counters.RecordEnqueued(id);
        }

        var stopwatch = Stopwatch.StartNew();
        using var reportingCts = new CancellationTokenSource();
        var reportingTask = PeriodicMetrics.RunAsync(
            metadata,
            stopwatch,
            benchmarkOptions.ReportInterval,
            () => MetricSnapshot.From(dispatcher.GetStats()),
            () => CreateMeasurements(counters, handler, observations),
            observations,
            reportingCts.Token);

        await produceAsync(dispatcher, EnqueueNext, stopwatch).ConfigureAwait(false);
        await dispatcher.CompleteAsync().ConfigureAwait(false);

        reportingCts.Cancel();
        await reportingTask.ConfigureAwait(false);
        stopwatch.Stop();

        var finalStats = MetricSnapshot.From(dispatcher.GetStats());
        observations.Observe(finalStats);
        var measurements = CreateMeasurements(counters, handler, observations);
        var status = ValidateKeyed(
            finalStats,
            measurements,
            handler,
            maxParallelism);

        return new ScenarioResult
        {
            Metadata = metadata,
            Elapsed = stopwatch.Elapsed,
            FinalStats = finalStats,
            Measurements = measurements,
            Status = status
        };
    }

    private static async Task<ScenarioResult> RunMessageSustainedAsync(
        ScenarioMetadata metadata,
        BenchmarkOptions benchmarkOptions,
        CpuWorkload[] workloads,
        int targetPending,
        int maxParallelism)
    {
        var observations = new ScalingObservations(maxParallelism);
        var transformer = new CpuTransformer(workloads);
        var subscriber = new CpuSubscriber();
        var counters = new RunCounters();
        var dispatcherOptions = CreateDispatcherOptions(
            benchmarkOptions,
            maxParallelism,
            observations);

        await using var dispatcher = new MessageDispatcher<CpuInput, CpuOutput>(dispatcherOptions);
        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        var nextId = 0L;
        void EnqueueNext()
        {
            var id = nextId++;
            var workloadIndex = (int)(id % workloads.Length);
            var salt = unchecked((int)(id * 7919) ^ workloadIndex * 104729);
            dispatcher.Enqueue(new CpuInput(id, workloadIndex, salt));
            counters.RecordEnqueued(id);
        }

        var stopwatch = Stopwatch.StartNew();
        using var reportingCts = new CancellationTokenSource();
        var reportingTask = PeriodicMetrics.RunAsync(
            metadata,
            stopwatch,
            benchmarkOptions.ReportInterval,
            () => MetricSnapshot.From(dispatcher.GetStats()),
            () => CreateMeasurements(counters, transformer, subscriber, observations),
            observations,
            reportingCts.Token);

        await ProduceToBacklogAsync(
            () => dispatcher.GetStats().PendingMessages,
            EnqueueNext,
            targetPending,
            stopwatch,
            benchmarkOptions.Duration).ConfigureAwait(false);

        await dispatcher.CompleteAsync().ConfigureAwait(false);

        reportingCts.Cancel();
        await reportingTask.ConfigureAwait(false);
        stopwatch.Stop();

        var finalStats = MetricSnapshot.From(dispatcher.GetStats());
        observations.Observe(finalStats);
        var measurements = CreateMeasurements(counters, transformer, subscriber, observations);
        var status = ValidateMessage(
            finalStats,
            measurements,
            counters,
            transformer,
            subscriber,
            maxParallelism);

        return new ScenarioResult
        {
            Metadata = metadata,
            Elapsed = stopwatch.Elapsed,
            FinalStats = finalStats,
            Measurements = measurements,
            Status = status
        };
    }

    private static DispatcherOptions CreateDispatcherOptions(
        BenchmarkOptions benchmarkOptions,
        int maxParallelism,
        ScalingObservations observations)
    {
        var sampleMilliseconds = benchmarkOptions.ScalingSampleInterval.TotalMilliseconds;

        return new DispatcherOptions
        {
            Parallelism = 1,
            MaxParallelism = maxParallelism,
            KeyBatchSize = BenchmarkDefaults.KeyBatchSize,
            ScaleObserver = observations.Observe,
            DynamicScaling = new DynamicScalingOptions
            {
                SampleInterval = benchmarkOptions.ScalingSampleInterval,
                MinimumUsefulThroughputGain = 0.02,
                ThroughputSmoothingFactor = 0.25,
                ProbeWarmupSamples = 1,
                ScaleUpCooldown = TimeSpan.FromMilliseconds(Math.Max(500, sampleMilliseconds * 2)),
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(Math.Max(750, sampleMilliseconds * 3))
            }
        };
    }

    private static async Task ProduceToBacklogAsync(
        Func<long> getPending,
        Action enqueueNext,
        int targetPending,
        Stopwatch stopwatch,
        TimeSpan endTime)
    {
        while (stopwatch.Elapsed < endTime)
        {
            var needed = targetPending - getPending();
            if (needed <= 0)
            {
                await Task.Delay(BenchmarkDefaults.ProducerPauseMilliseconds).ConfigureAwait(false);
                continue;
            }

            var batchSize = (int)Math.Min(needed, BenchmarkDefaults.ProducerBatchSize);
            for (var index = 0; index < batchSize; index++)
            {
                enqueueNext();
            }

            await Task.Yield();
        }
    }

    private static RuntimeMeasurements CreateMeasurements(
        RunCounters counters,
        OrderedCpuHandler handler,
        ScalingObservations observations)
    {
        return new RuntimeMeasurements(
            counters.Enqueued,
            handler.Processed,
            handler.Checksum,
            observations.PeakWorkers,
            handler.MaxConcurrency,
            handler.OrderingViolations,
            handler.SameKeyConcurrencyViolations,
            observations.WorkerBoundViolations);
    }

    private static RuntimeMeasurements CreateMeasurements(
        RunCounters counters,
        CpuTransformer transformer,
        CpuSubscriber subscriber,
        ScalingObservations observations)
    {
        return new RuntimeMeasurements(
            counters.Enqueued,
            subscriber.Published,
            subscriber.Checksum,
            observations.PeakWorkers,
            transformer.MaxConcurrency,
            0,
            0,
            observations.WorkerBoundViolations);
    }

    private static string ValidateKeyed(
        MetricSnapshot stats,
        RuntimeMeasurements measurements,
        OrderedCpuHandler handler,
        int maxParallelism)
    {
        var failures = new List<string>();
        ValidateCommon(stats, measurements, maxParallelism, failures);

        if (handler.HandlerErrors != 0)
        {
            failures.Add($"handler-errors={handler.HandlerErrors}");
        }

        if (measurements.OrderingViolations != 0)
        {
            failures.Add($"ordering={measurements.OrderingViolations}");
        }

        if (measurements.SameKeyConcurrencyViolations != 0)
        {
            failures.Add($"same-key-concurrency={measurements.SameKeyConcurrencyViolations}");
        }

        if (handler.FirstViolation is not null)
        {
            failures.Add(handler.FirstViolation);
        }

        return BuildStatus(failures);
    }

    private static string ValidateMessage(
        MetricSnapshot stats,
        RuntimeMeasurements measurements,
        RunCounters counters,
        CpuTransformer transformer,
        CpuSubscriber subscriber,
        int maxParallelism)
    {
        var failures = new List<string>();
        ValidateCommon(stats, measurements, maxParallelism, failures);

        if (transformer.TransformErrors != 0)
        {
            failures.Add($"transform-errors={transformer.TransformErrors}");
        }

        if (subscriber.SubscriberErrors != 0)
        {
            failures.Add($"subscriber-errors={subscriber.SubscriberErrors}");
        }

        if (subscriber.Published != counters.Enqueued)
        {
            failures.Add($"published={subscriber.Published}");
        }

        if (subscriber.IdSum != counters.EnqueuedIdSum)
        {
            failures.Add($"id-sum={subscriber.IdSum}-expected={counters.EnqueuedIdSum}");
        }

        if (subscriber.Checksum != transformer.Checksum)
        {
            failures.Add("checksum-mismatch");
        }

        if (transformer.FirstError is not null)
        {
            failures.Add(transformer.FirstError);
        }

        return BuildStatus(failures);
    }

    private static void ValidateCommon(
        MetricSnapshot stats,
        RuntimeMeasurements measurements,
        int maxParallelism,
        List<string> failures)
    {
        if (stats.PendingMessages != 0)
        {
            failures.Add($"pending={stats.PendingMessages}");
        }

        if (stats.CompletedMessages != measurements.Enqueued)
        {
            failures.Add($"completed={stats.CompletedMessages}-expected={measurements.Enqueued}");
        }

        if (measurements.Processed != measurements.Enqueued)
        {
            failures.Add($"processed={measurements.Processed}-expected={measurements.Enqueued}");
        }

        if (measurements.PeakWorkers > maxParallelism)
        {
            failures.Add($"peak-workers={measurements.PeakWorkers}-max={maxParallelism}");
        }

        if (measurements.MaxProcessingConcurrency > maxParallelism)
        {
            failures.Add(
                $"processing-concurrency={measurements.MaxProcessingConcurrency}-max={maxParallelism}");
        }

        if (measurements.WorkerBoundViolations != 0)
        {
            failures.Add($"worker-bound-violations={measurements.WorkerBoundViolations}");
        }
    }

    private static string BuildStatus(List<string> failures)
    {
        return failures.Count == 0
            ? "PASS"
            : $"FAIL:{string.Join('|', failures)}";
    }
}
