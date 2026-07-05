using System.Collections.Concurrent;
using MessageDispatching;

var handler = new SampleHandler();

await using var dispatcher = new KeyedOrderedDispatcher<string, MessageEnvelope>(
    handler,
    new KeyedOrderedDispatcherOptions
    {
        Parallelism = 4,
        BatchSize = 2
    });

for (var i = 1; i <= 8; i++)
{
    dispatcher.Enqueue("hot-a", new MessageEnvelope(i, 35));
    dispatcher.Enqueue("hot-b", new MessageEnvelope(i, 35));
    dispatcher.Enqueue("cold-c", new MessageEnvelope(i, 10));
}

await dispatcher.CompleteAsync();

Console.WriteLine($"max concurrency observed: {handler.MaxConcurrency}");

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
