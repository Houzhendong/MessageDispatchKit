namespace MessageDispatching;

public readonly record struct DispatcherStats(
    int PendingMessages,
    int KnownPartitions,
    int WorkerCount,
    int BusyWorkers,
    int QueuedWorkItems,
    bool IsAccepting);
