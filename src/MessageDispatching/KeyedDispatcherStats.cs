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
    long ProbeAcceptCount,
    long ProbeRejectCount,
    double? LastProbeGain,
    bool Accepting);
