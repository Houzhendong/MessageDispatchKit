using System.Collections.Concurrent;
using MessageDispatching;

var handler = new SampleHandler();

await using var dispatcher = new KeyedOrderedDispatcher<string, MessageEnvelope>(
    new DispatcherOptions
    {
        Parallelism = 1,
        MaxParallelism = 4,
        KeyBatchSize = 2,
        ScaleInterval = TimeSpan.FromMilliseconds(10),
        ScaleUpCooldown = TimeSpan.FromMilliseconds(10),
        ScaleDownIdleDuration = TimeSpan.FromMilliseconds(100),
        ScaleUpQueuedWorkItemsThreshold = 1,
        ScaleUpMessagesPerWorkerThreshold = 2,
        ScaleUpConsecutiveSamples = 1
    });

dispatcher.Start(handler);

for (var i = 1; i <= 12; i++)
{
    dispatcher.Enqueue("hot-a", new MessageEnvelope(i, 100));
    dispatcher.Enqueue("hot-b", new MessageEnvelope(i, 100));
    dispatcher.Enqueue("cold-c", new MessageEnvelope(i, 40));
}

var peakWorkers = dispatcher.GetStats().WorkerCount;
var deadline = DateTimeOffset.UtcNow.AddSeconds(5);

while (true)
{
    await Task.Delay(25);

    var stats = dispatcher.GetStats();
    peakWorkers = Math.Max(peakWorkers, stats.WorkerCount);

    if (stats.PendingMessages == 0 && stats.WorkerCount == 1)
    {
        break;
    }

    if (DateTimeOffset.UtcNow > deadline)
    {
        throw new TimeoutException(
            $"Timed out waiting for scale down. Pending={stats.PendingMessages}, Workers={stats.WorkerCount}");
    }
}

Console.WriteLine($"max concurrency observed: {handler.MaxConcurrency}");
Console.WriteLine($"peak workers observed: {peakWorkers}");
Console.WriteLine($"workers after scale down: {dispatcher.GetStats().WorkerCount}");

await dispatcher.CompleteAsync();

await NoKeySample.RunAsync();

public readonly record struct MessageEnvelope(int Sequence, int WorkMs);

public sealed class SampleHandler : IKeyedMessageHandler<string, MessageEnvelope>
{
    private readonly ConcurrentDictionary<string, int> _lastSequenceByKey = new();
    private int _currentConcurrency;
    private int _maxConcurrency;

    public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

    public void Handle(string key, MessageEnvelope message, CancellationToken cancellationToken)
    {
        var running = Interlocked.Increment(ref _currentConcurrency);
        RecordMaxConcurrency(running);

        try
        {
            Thread.Sleep(message.WorkMs);

            _lastSequenceByKey.AddOrUpdate(
                key,
                message.Sequence,
                (_, previous) =>
                {
                    if (message.Sequence != previous + 1)
                    {
                        throw new InvalidOperationException(
                            $"Out of order for key {key}: previous={previous}, current={message.Sequence}");
                    }

                    return message.Sequence;
                });

            Console.WriteLine($"handled key={key}, seq={message.Sequence}");
        }
        finally
        {
            Interlocked.Decrement(ref _currentConcurrency);
        }
    }

    private void RecordMaxConcurrency(int value)
    {
        while (true)
        {
            var snapshot = Volatile.Read(ref _maxConcurrency);
            if (value <= snapshot)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _maxConcurrency, value, snapshot) == snapshot)
            {
                return;
            }
        }
    }

    public void HandleError(string key, MessageEnvelope message, Exception exception, CancellationToken cancellationToken)
    {
        Console.Error.WriteLine($"failed key={key}, seq={message.Sequence}: {exception.Message}");
    }
}
