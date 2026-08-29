using System.Diagnostics;
using MessageDispatching;

public static class NoKeySample
{
    public static async Task RunAsync()
    {
        var transformer = new NoKeySampleTransformer();
        var subscriber = new NoKeySampleSubscriber();

        await using var dispatcher = new MessageDispatcher<RawPacket, ParsedEvent>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 4,
                ScaleInterval = TimeSpan.FromMilliseconds(10),
                ScaleUpCooldown = TimeSpan.FromMilliseconds(10),
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(100),
                ScaleUpQueuedWorkItemsThreshold = 0,
                ScaleUpConsecutiveSamples = 1,
                ScaleObserver = static change =>
                    Console.WriteLine(
                        $"no-key scale {(change.IsScaleUp ? "up" : "down")}: " +
                        $"{change.PreviousWorkerCount} -> {change.CurrentWorkerCount}, " +
                        $"pending={change.Stats.PendingMessages}, queued={change.Stats.QueuedWorkItems}")
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        for (var i = 1; i <= 24; i++)
        {
            dispatcher.Enqueue(new RawPacket(i, 100));
        }

        var peakWorkers = dispatcher.GetStats().WorkerCount;
        var startTimestamp = Stopwatch.GetTimestamp();

        while (true)
        {
            await Task.Delay(25);

            var stats = dispatcher.GetStats();
            peakWorkers = Math.Max(peakWorkers, stats.WorkerCount);

            if (stats.PendingMessages == 0 && stats.WorkerCount == 1)
            {
                break;
            }

            if (Stopwatch.GetElapsedTime(startTimestamp) > TimeSpan.FromSeconds(5))
            {
                throw new TimeoutException(
                    $"No-key dispatcher timed out waiting for scale down. Pending={stats.PendingMessages}, Workers={stats.WorkerCount}");
            }
        }

        Console.WriteLine($"no-key transform concurrency observed: {transformer.MaxConcurrency}");
        Console.WriteLine($"no-key published count: {subscriber.PublishedCount}");
        Console.WriteLine($"no-key peak workers observed: {peakWorkers}");
        Console.WriteLine($"no-key workers after scale down: {dispatcher.GetStats().WorkerCount}");

        await dispatcher.CompleteAsync();

        await RunSingleWorkerAsync();
    }

    private static async Task RunSingleWorkerAsync()
    {
        var transformer = new NoKeySampleTransformer();
        var subscriber = new NoKeySampleSubscriber();

        await using var dispatcher = new MessageDispatcher<RawPacket, ParsedEvent>(
            new DispatcherOptions
            {
                Parallelism = 1
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        for (var i = 1; i <= 4; i++)
        {
            dispatcher.Enqueue(new RawPacket(i, 5));
        }

        await dispatcher.CompleteAsync();

        Console.WriteLine($"no-key mpsc published count: {subscriber.PublishedCount}");
        Console.WriteLine($"no-key mpsc max concurrency observed: {transformer.MaxConcurrency}");
    }
}

public readonly record struct RawPacket(int Id, int ParseMs);

public readonly record struct ParsedEvent(int Id, string Payload);

public sealed class NoKeySampleTransformer : IMessageTransformer<RawPacket, ParsedEvent>
{
    private int _currentConcurrency;
    private int _maxConcurrency;

    public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

    public ParsedEvent Transform(RawPacket input, CancellationToken cancellationToken)
    {
        var running = Interlocked.Increment(ref _currentConcurrency);
        RecordMaxConcurrency(running);

        try
        {
            Thread.Sleep(input.ParseMs);
            return new ParsedEvent(input.Id, $"payload-{input.Id}");
        }
        finally
        {
            Interlocked.Decrement(ref _currentConcurrency);
        }
    }

    public void HandleError(RawPacket input, Exception exception, CancellationToken cancellationToken)
    {
        Console.Error.WriteLine($"parse failed id={input.Id}: {exception.Message}");
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
}

public sealed class NoKeySampleSubscriber : IMessageSubscriber<ParsedEvent>
{
    private int _publishedCount;

    public int PublishedCount => Volatile.Read(ref _publishedCount);

    public void Handle(ParsedEvent message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _publishedCount);
    }

    public void HandleError(ParsedEvent message, Exception exception, CancellationToken cancellationToken)
    {
        Console.Error.WriteLine($"subscriber failed id={message.Id}: {exception.Message}");
    }
}
