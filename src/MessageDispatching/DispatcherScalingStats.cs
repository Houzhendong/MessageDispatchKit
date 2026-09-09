namespace MessageDispatching;

public readonly record struct DispatcherScalingStats(
    long PendingMessages,
    long CompletedMessages,
    int WorkerCount,
    int DesiredWorkerCount,
    int BusyWorkers,
    long ReadyWorkItemCount,
    double Throughput,
    double SmoothedThroughput,
    bool IsSaturated,
    long ScaleUpCount,
    long ScaleDownCount,
    long ProbeAcceptCount,
    long ProbeRejectCount,
    double? LastProbeGain,
    bool Accepting);
