namespace MessageDispatching;

public sealed class DispatcherScaleChange
{
    internal DispatcherScaleChange(
        int previousWorkerCount,
        int currentWorkerCount,
        DispatcherStats stats)
    {
        PreviousWorkerCount = previousWorkerCount;
        CurrentWorkerCount = currentWorkerCount;
        Stats = stats with { WorkerCount = currentWorkerCount };
    }

    public int PreviousWorkerCount { get; }

    public int CurrentWorkerCount { get; }

    public bool IsScaleUp => CurrentWorkerCount > PreviousWorkerCount;

    public DispatcherStats Stats { get; }
}
