using MessageDispatching;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class KeyedOrderedDispatcherTests
{
    [Fact]
    public async Task SameKeyMessagesAreHandledInEnqueueOrder()
    {
        var handler = new RecordingKeyedHandler();
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 4,
                KeyBatchSize = 3
            });

        dispatcher.Start(handler);

        for (var i = 1; i <= 50; i++)
        {
            dispatcher.Enqueue("a", i);
        }

        await dispatcher.CompleteAsync();

        Assert.Equal(Enumerable.Range(1, 50), handler.GetMessages("a"));
    }

    [Fact]
    public async Task DifferentKeysCanRunConcurrently()
    {
        var handler = new DelayingKeyedHandler(TimeSpan.FromMilliseconds(25));
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 2,
                KeyBatchSize = 1
            });

        dispatcher.Start(handler);

        for (var i = 0; i < 10; i++)
        {
            dispatcher.Enqueue("a", i);
            dispatcher.Enqueue("b", i);
        }

        await dispatcher.CompleteAsync();

        Assert.True(handler.MaxConcurrency > 1);
    }

    [Fact]
    public async Task HandlerErrorIsReportedAndProcessingContinues()
    {
        var handler = new ThrowingKeyedHandler(messageToThrow: 2);
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 1,
                KeyBatchSize = 1
            });

        dispatcher.Start(handler);
        dispatcher.Enqueue("a", 1);
        dispatcher.Enqueue("a", 2);
        dispatcher.Enqueue("a", 3);

        await dispatcher.CompleteAsync();

        Assert.Equal(new[] { 1, 2, 3 }, handler.Attempts);
        Assert.Equal(new[] { 2 }, handler.Errors);
    }

    [Fact]
    public async Task CompleteAsyncDrainsQueuedMessages()
    {
        var handler = new RecordingKeyedHandler();
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 2,
                KeyBatchSize = 2
            });

        dispatcher.Start(handler);

        for (var i = 0; i < 40; i++)
        {
            dispatcher.Enqueue((i % 4).ToString(), i);
        }

        await dispatcher.CompleteAsync();

        Assert.Equal(40, handler.TotalCount);
        Assert.Equal(0, dispatcher.GetStats().PendingMessages);
        Assert.False(dispatcher.GetStats().IsAccepting);
    }

    [Fact]
    public async Task DynamicWorkersScaleUpAndBackDown()
    {
        var handler = new DelayingKeyedHandler(TimeSpan.FromMilliseconds(40));
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 4,
                KeyBatchSize = 1,
                ScaleInterval = TimeSpan.FromMilliseconds(5),
                ScaleUpCooldown = TimeSpan.FromMilliseconds(5),
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(50),
                ScaleUpQueuedWorkItemsThreshold = 1,
                ScaleUpMessagesPerWorkerThreshold = 2,
                ScaleUpConsecutiveSamples = 1
            });

        dispatcher.Start(handler);

        for (var i = 0; i < 20; i++)
        {
            dispatcher.Enqueue("a", i);
            dispatcher.Enqueue("b", i);
            dispatcher.Enqueue("c", i);
        }

        await TestWait.UntilAsync(() => dispatcher.GetStats().WorkerCount > 1);
        await TestWait.UntilAsync(() => dispatcher.GetStats().PendingMessages == 0);
        await TestWait.UntilAsync(() => dispatcher.GetStats().WorkerCount == 1);

        Assert.True(handler.MaxConcurrency > 1);

        await dispatcher.CompleteAsync();
    }

    [Fact]
    public async Task EnqueueBeforeStartThrows()
    {
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>();

        Assert.Throws<InvalidOperationException>(() => dispatcher.Enqueue("a", 1));
    }

    private sealed class RecordingKeyedHandler : IKeyedMessageHandler<string, int>
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, List<int>> _messagesByKey = new();

        public int TotalCount
        {
            get
            {
                lock (_gate)
                {
                    return _messagesByKey.Values.Sum(static messages => messages.Count);
                }
            }
        }

        public void Handle(string key, int message, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (!_messagesByKey.TryGetValue(key, out var messages))
                {
                    messages = new List<int>();
                    _messagesByKey.Add(key, messages);
                }

                messages.Add(message);
            }
        }

        public IReadOnlyList<int> GetMessages(string key)
        {
            lock (_gate)
            {
                return _messagesByKey.TryGetValue(key, out var messages)
                    ? messages.ToArray()
                    : Array.Empty<int>();
            }
        }
    }

    private sealed class DelayingKeyedHandler : IKeyedMessageHandler<string, int>
    {
        private readonly TimeSpan _delay;
        private int _currentConcurrency;
        private int _maxConcurrency;

        public DelayingKeyedHandler(TimeSpan delay) => _delay = delay;

        public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

        public void Handle(string key, int message, CancellationToken cancellationToken)
        {
            var running = Interlocked.Increment(ref _currentConcurrency);
            RecordMaxConcurrency(running);

            try
            {
                Thread.Sleep(_delay);
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
    }

    private sealed class ThrowingKeyedHandler : IKeyedMessageHandler<string, int>
    {
        private readonly int _messageToThrow;
        private readonly object _gate = new();
        private readonly List<int> _attempts = new();
        private readonly List<int> _errors = new();

        public ThrowingKeyedHandler(int messageToThrow) => _messageToThrow = messageToThrow;

        public IReadOnlyList<int> Attempts
        {
            get
            {
                lock (_gate)
                {
                    return _attempts.ToArray();
                }
            }
        }

        public IReadOnlyList<int> Errors
        {
            get
            {
                lock (_gate)
                {
                    return _errors.ToArray();
                }
            }
        }

        public void Handle(string key, int message, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _attempts.Add(message);
            }

            if (message == _messageToThrow)
            {
                throw new InvalidOperationException("Expected test failure.");
            }
        }

        public void HandleError(string key, int message, Exception exception, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _errors.Add(message);
            }
        }
    }
}
