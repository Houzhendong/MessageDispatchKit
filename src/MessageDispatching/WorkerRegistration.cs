using System.Diagnostics;

namespace MessageDispatching;

internal sealed class WorkerRegistration : IDisposable
{
    public readonly CancellationTokenSource RetirementCts = new();
    public Task WorkerTask { get; set; } = Task.CompletedTask;
    public bool IsWaiting;
    public bool RetirementRequested;
    private readonly object _metricsLock = new();
    private long _busyStarted;
    private long _busyTicks;

    public void BeginProcessing()
    {
        lock (_metricsLock)
        {
            _busyStarted = Stopwatch.GetTimestamp();
        }
    }

    public void EndProcessing()
    {
        lock (_metricsLock)
        {
            _busyTicks += Stopwatch.GetTimestamp() - _busyStarted;
            _busyStarted = 0;
        }
    }

    public long ReadBusyTicks()
    {
        lock (_metricsLock)
        {
            return _busyTicks + (_busyStarted == 0 ? 0 : Stopwatch.GetTimestamp() - _busyStarted);
        }
    }

    public void Dispose() => RetirementCts.Dispose();
}
