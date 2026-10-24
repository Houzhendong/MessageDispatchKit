namespace MessageDispatching;

public readonly record struct KeyedDispatcherStats(
    long PendingMessages,
    long CompletedMessages,
    int KeyCount,
    int WorkerCount,
    int DesiredWorkerCount,
    int BusyWorkers,
    int ReadyKeyCount,
    double Throughput,
    double SmoothedThroughput,
    bool IsSaturated,
    long ScaleUpCount,
    long ScaleDownCount,
    bool Accepting)
{
    public int ActiveKeyCount { get; init; }
    public double AverageBusyWorkers { get; init; }
    public double ReadyKeyWaitMilliseconds { get; init; }
    public int CapacityLimit { get; init; }
    public double ProcessCpuUtilization { get; init; }
}
