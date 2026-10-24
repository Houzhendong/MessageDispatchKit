namespace MessageDispatching;

public readonly record struct DispatcherStats(
    long PendingMessages,
    long CompletedMessages,
    int WorkerCount,
    int DesiredWorkerCount,
    int BusyWorkers,
    long QueuedMessageCount,
    double Throughput,
    double SmoothedThroughput,
    bool IsSaturated,
    long ScaleUpCount,
    long ScaleDownCount,
    bool Accepting)
{
    public double AverageBusyWorkers { get; init; }
    public double QueueWaitMilliseconds { get; init; }
    public int CapacityLimit { get; init; }
    public double ProcessCpuUtilization { get; init; }
}
