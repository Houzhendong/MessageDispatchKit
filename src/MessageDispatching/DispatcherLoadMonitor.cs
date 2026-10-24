using System.Diagnostics;

namespace MessageDispatching;

// Called under the dispatcher's worker lock, except Start before any worker exists.
internal sealed class DispatcherLoadMonitor : IDisposable
{
    private readonly Process _process = Process.GetCurrentProcess();
    private long _retiredBusyTicks;
    private long _lastBusyTicks;
    private long _lastTimestamp;
    private TimeSpan _lastCpuTime;

    public void Start()
    {
        _lastTimestamp = Stopwatch.GetTimestamp();
        _lastCpuTime = _process.TotalProcessorTime;
    }

    public void Retire(WorkerRegistration worker) => _retiredBusyTicks += worker.ReadBusyTicks();

    public LoadSnapshot Sample(IReadOnlyList<WorkerRegistration> workers,
        int workerCount, int desiredWorkerCount, long runnableWorkCount,
        long readyWorkCount, long completedMessages, long? oldestReadyTimestamp)
    {
        var busyTicks = _retiredBusyTicks;
        foreach (var worker in workers) busyTicks += worker.ReadBusyTicks();
        var timestamp = Stopwatch.GetTimestamp();
        var elapsed = Stopwatch.GetElapsedTime(_lastTimestamp, timestamp).TotalSeconds;
        var busy = elapsed > 0 ? (busyTicks - _lastBusyTicks) / (elapsed * Stopwatch.Frequency) : 0;
        var cpuTime = _process.TotalProcessorTime;
        var cpu = elapsed > 0
            ? Math.Clamp((cpuTime - _lastCpuTime).TotalSeconds / elapsed / Environment.ProcessorCount, 0, 1)
            : 0;
        _lastTimestamp = timestamp;
        _lastBusyTicks = busyTicks;
        _lastCpuTime = cpuTime;
        var wait = oldestReadyTimestamp.HasValue
            ? Stopwatch.GetElapsedTime(oldestReadyTimestamp.Value, Math.Max(oldestReadyTimestamp.Value, timestamp))
            : TimeSpan.Zero;
        return new LoadSnapshot(workerCount, desiredWorkerCount, runnableWorkCount,
            readyWorkCount, busy, wait, completedMessages, cpu, timestamp);
    }

    public void Dispose() => _process.Dispose();
}
