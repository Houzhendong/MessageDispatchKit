using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using MessageDispatching;

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
var arguments = args.Chunk(2).ToDictionary(pair => pair[0], pair => pair[1]);
if (arguments.ContainsKey("--calibrate"))
{
    // One calibration is shared by every old/new run. The handlers do fixed CPU work;
    // elapsed time controls arrival scheduling only, never how much work a message does.
    var buffer = Work.CreateBuffer();
    Work.Execute(buffer, 20, 0);
    var watch = Stopwatch.StartNew();
    for (var i = 0; i < 100; i++) Work.Execute(buffer, 20, i);
    var workPasses = Math.Max(1, (int)Math.Round(20 * 0.0005 / (watch.Elapsed.TotalSeconds / 100)));
    var count = 0L;
    var stop = false;
    var calibrationThreads = Enumerable.Range(0, Math.Min(8, Math.Max(2, Environment.ProcessorCount)))
        .Select(index => new Thread(() =>
        {
            while (!Volatile.Read(ref stop))
            {
                Work.Execute(buffer, workPasses, index);
                Interlocked.Increment(ref count);
            }
        }) { IsBackground = true }).ToArray();
    foreach (var thread in calibrationThreads) thread.Start();
    Thread.Sleep(3000);
    var before = Interlocked.Read(ref count);
    watch.Restart();
    Thread.Sleep(2000);
    var measuredCapacity = (Interlocked.Read(ref count) - before) / watch.Elapsed.TotalSeconds;
    Volatile.Write(ref stop, true);
    foreach (var thread in calibrationThreads) thread.Join();
    Console.WriteLine(JsonSerializer.Serialize(new { WorkPasses = workPasses, Capacity = measuredCapacity }));
    return;
}

var scenario = arguments["--scenario"];
var version = arguments["--version"];
var output = arguments["--output"];
var passes = int.Parse(arguments["--work"]);
var duration = arguments.TryGetValue("--duration", out var seconds) ? double.Parse(seconds) : 8;
var capacity = double.Parse(arguments["--capacity"]);
var keyCount = scenario == "one-hot-key" ? 1 : 32;
var maximum = Math.Min(8, Math.Max(2, Environment.ProcessorCount));
var handler = new Handler(keyCount, passes);
var changes = new ConcurrentQueue<(double Seconds, int Workers)>();
var clock = new Stopwatch();
var options = new DispatcherOptions
{
    Parallelism = 1,
    MaxParallelism = maximum,
    KeyBatchSize = 8,
    DynamicScaling = new DynamicScalingOptions
    {
        SampleInterval = TimeSpan.FromMilliseconds(100),
        ScaleUpCooldown = TimeSpan.FromMilliseconds(200),
        ScaleDownIdleDuration = TimeSpan.FromSeconds(1),
        ThroughputSmoothingFactor = 0.25,
        ProbeWarmupSamples = 1
    },
    ScaleObserver = change => changes.Enqueue((clock.Elapsed.TotalSeconds, change.CurrentWorkerCount))
};
// This file only uses the baseline public API, including positional stats members.
await using var dispatcher = new KeyedOrderedDispatcher<int, Message>(options);
handler.Warmup();
using var process = Process.GetCurrentProcess();
var initialCpu = process.TotalProcessorTime;
dispatcher.Start(handler);
var samples = new List<Sample>();
long enqueued = 0;
var producerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var sequences = new int[keyCount];
var started = new ManualResetEventSlim();
var producer = new Thread(() =>
{
    started.Wait();
    try
    {
        // An identical piecewise arrival trace is replayed in both implementations.
        // A dedicated producer avoids tying arrival rate to available ThreadPool threads.
        var next = 0d;
        while (next < duration)
        {
            var remaining = next - clock.Elapsed.TotalSeconds;
            if (remaining > 0.002) { Thread.Sleep(1); continue; }
            if (remaining > 0) { Thread.Yield(); continue; }
            var key = (int)(enqueued % keyCount);
            var factor = scenario == "heterogeneous" ? ((enqueued % 4) switch
                { 0 => 1, 1 => 2, 2 => 4, _ => 9 }) : 1;
            dispatcher.Enqueue(key, new Message(sequences[key]++, Stopwatch.GetTimestamp(), factor));
            enqueued++;
            next += 1 / ArrivalRate(scenario, next / duration, capacity);
        }
        producerFinished.SetResult();
    }
    catch (Exception exception) { producerFinished.SetException(exception); }
}) { IsBackground = true, Name = "comparison-arrivals" };
producer.Start();
clock.Start();
started.Set();
while (!producerFinished.Task.IsCompleted)
{
    Capture();
    await Task.Delay(50);
}
await producerFinished.Task;
var offeredSeconds = clock.Elapsed.TotalSeconds;
var complete = dispatcher.CompleteAsync().AsTask();
while (!complete.IsCompleted)
{
    Capture();
    await Task.WhenAny(complete, Task.Delay(50));
    if (clock.Elapsed.TotalSeconds > duration + 60)
        throw new TimeoutException("Drain exceeded 60 seconds.");
}
await complete;
clock.Stop();
Capture();
started.Dispose();
var stats = dispatcher.GetStats();
if (stats.PendingMessages != 0 || stats.CompletedMessages != enqueued || handler.Processed != enqueued ||
    handler.OrderingViolations != 0 || handler.ConcurrencyViolations != 0 || handler.Errors != 0)
    throw new InvalidOperationException("Message count, ordering, concurrency, or handler error validation failed.");
if (samples.Any(sample => sample.Workers > maximum) ||
    (scenario == "one-hot-key" && changes.Any(change => change.Workers > 1)))
    throw new InvalidOperationException("Worker count exceeded the resource or single-key bound.");

var workerSeconds = 0d;
var previousTime = 0d;
var previousWorkers = 1;
foreach (var change in changes.OrderBy(change => change.Seconds))
{
    workerSeconds += Math.Max(0, change.Seconds - previousTime) * previousWorkers;
    previousTime = change.Seconds;
    previousWorkers = change.Workers;
}
workerSeconds += Math.Max(0, clock.Elapsed.TotalSeconds - previousTime) * previousWorkers;
var wait = handler.WaitTimes.Order().ToArray();
var result = new
{
    Version = version, Scenario = scenario, WorkPasses = passes, LogicalProcessors = Environment.ProcessorCount,
    MaximumWorkers = maximum, Enqueued = enqueued, handler.Processed,
    OfferedSeconds = offeredSeconds, TotalSeconds = clock.Elapsed.TotalSeconds,
    Throughput = enqueued / clock.Elapsed.TotalSeconds,
    ProcessCpuSeconds = (process.TotalProcessorTime - initialCpu).TotalSeconds,
    DrainSeconds = Math.Max(0, clock.Elapsed.TotalSeconds - offeredSeconds),
    P50WaitMs = Percentile(wait, 0.50), P95WaitMs = Percentile(wait, 0.95), P99WaitMs = Percentile(wait, 0.99),
    WorkerSeconds = workerSeconds,
    PeakWorkers = changes.Select(change => change.Workers).Append(1).Max(),
    LateAverageWorkers = samples.Where(sample => sample.Seconds >= duration * 0.75 && sample.Seconds <= duration)
        .Select(sample => (double)sample.Workers).DefaultIfEmpty(1).Average(),
    stats.ScaleUpCount, stats.ScaleDownCount, stats.ProbeAcceptCount, stats.ProbeRejectCount,
    handler.OrderingViolations, handler.ConcurrencyViolations, handler.Errors,
    Checksum = handler.Checksum, Valid = true
};
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output))!);
File.WriteAllText(output, JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
File.WriteAllLines(Path.ChangeExtension(output, ".csv"),
    new[] { "seconds,workers,busy,ready,pending,completed" }.Concat(samples.Select(sample =>
        $"{sample.Seconds:F4},{sample.Workers},{sample.Busy},{sample.Ready},{sample.Pending},{sample.Completed}")));
Console.WriteLine(JsonSerializer.Serialize(result));

void Capture()
{
    var sample = dispatcher.GetStats();
    samples.Add(new Sample(clock.Elapsed.TotalSeconds, sample.WorkerCount, sample.BusyWorkers,
        sample.ReadyKeyCount, sample.PendingMessages, sample.CompletedMessages));
}

static double ArrivalRate(string scenario, double phase, double capacity) => scenario switch
{
    "burst" => phase < 0.2 ? capacity * 0.02 : phase < 0.65 ? capacity * 0.8 : capacity * 0.02,
    "partial-load" => phase < 0.4 ? capacity * 0.8 : capacity * 0.25,
    "one-hot-key" => capacity * 0.8,
    "heterogeneous" => capacity * 0.8 / 4,
    "cpu-saturation" => capacity * 1.5,
    _ => throw new ArgumentException($"Unknown scenario: {scenario}")
};
static double Percentile(long[] sorted, double percentile) => sorted.Length == 0 ? 0 :
    sorted[(int)Math.Clamp(Math.Ceiling(percentile * sorted.Length) - 1, 0, sorted.Length - 1)] * 1000d / Stopwatch.Frequency;

readonly record struct Message(int Sequence, long EnqueuedAt, int Factor);
readonly record struct Sample(double Seconds, int Workers, int Busy, int Ready, long Pending, long Completed);

sealed class Handler(int keyCount, int passes) : IKeyedMessageHandler<int, Message>
{
    private readonly byte[] _buffer = Work.CreateBuffer();
    private readonly int[] _active = new int[keyCount];
    private readonly int[] _sequences = new int[keyCount];
    public readonly ConcurrentQueue<long> WaitTimes = new();
    public long Processed;
    public long OrderingViolations;
    public long ConcurrencyViolations;
    public long Errors;
    public long Checksum;
    public void Warmup()
    {
        for (var i = 0; i < 100; i++) Work.Execute(_buffer, passes, i);
    }
    public void Handle(int key, Message message, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref _active[key]) != 1) Interlocked.Increment(ref ConcurrencyViolations);
        try
        {
            WaitTimes.Enqueue(Stopwatch.GetTimestamp() - message.EnqueuedAt);
            if (_sequences[key]++ != message.Sequence) Interlocked.Increment(ref OrderingViolations);
            var checksum = Work.Execute(_buffer, passes * message.Factor, message.Sequence);
            Interlocked.Add(ref Checksum, unchecked((long)checksum));
            Interlocked.Increment(ref Processed);
        }
        finally { Interlocked.Decrement(ref _active[key]); }
    }
    public void HandleError(int key, Message message, Exception exception, CancellationToken cancellationToken)
        => Interlocked.Increment(ref Errors);
}

static class Work
{
    public static byte[] CreateBuffer() => Enumerable.Range(0, 8192).Select(i => (byte)(i * 17 + 31)).ToArray();
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static ulong Execute(byte[] buffer, int passes, int salt)
    {
        var hash = unchecked((ulong)salt) + 14695981039346656037UL;
        for (var pass = 0; pass < passes; pass++)
            foreach (var value in buffer) hash = (hash ^ value) * 1099511628211UL;
        return hash;
    }
}
