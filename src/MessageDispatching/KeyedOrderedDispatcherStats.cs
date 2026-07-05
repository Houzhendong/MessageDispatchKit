namespace MessageDispatching;

public readonly record struct KeyedOrderedDispatcherStats(
    int PendingMessages,
    int KnownKeys,
    bool IsAccepting);
