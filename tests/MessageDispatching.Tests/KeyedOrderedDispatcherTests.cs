using System.Collections.Concurrent;
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
                ScaleUpQueuedWorkItemsThreshold = 0,
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
    public async Task ScaleUpRequiresQueuedWorkItemsToExceedThreshold()
    {
        using var handler = new BlockingKeyedHandler();
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 2,
                KeyBatchSize = 1,
                ScaleInterval = TimeSpan.FromMilliseconds(10),
                ScaleUpCooldown = TimeSpan.FromDays(365),
                ScaleDownIdleDuration = TimeSpan.FromSeconds(10),
                ScaleUpQueuedWorkItemsThreshold = 1,
                ScaleUpConsecutiveSamples = 1
            });

        dispatcher.Start(handler);

        try
        {
            dispatcher.Enqueue("active", 0);
            await TestWait.UntilAsync(() => handler.FirstStarted);
            await TestWait.UntilAsync(() =>
            {
                var stats = dispatcher.GetStats();
                return stats.WorkerCount == 1 &&
                    stats.BusyWorkers == 1 &&
                    stats.QueuedWorkItems == 0;
            });

            dispatcher.Enqueue("queued-1", 1);
            await TestWait.UntilAsync(() => dispatcher.GetStats().QueuedWorkItems == 1);

            await Task.Delay(TimeSpan.FromMilliseconds(100));

            var boundaryStats = dispatcher.GetStats();
            Assert.Equal(1, boundaryStats.WorkerCount);
            Assert.Equal(1, boundaryStats.BusyWorkers);
            Assert.Equal(1, boundaryStats.QueuedWorkItems);

            dispatcher.Enqueue("queued-2", 2);
            await TestWait.UntilAsync(() => dispatcher.GetStats().WorkerCount == 2);
        }
        finally
        {
            handler.Release();
            await dispatcher.CompleteAsync();
        }
    }

    [Fact]
    public async Task IdleDynamicWorkerRetiresWhileHotKeyStillHasPendingMessages()
    {
        using var handler = new GatedKeyedHandler();
        var scaleChanges = new ConcurrentQueue<DispatcherScaleChange>();
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 2,
                KeyBatchSize = 1,
                ScaleInterval = TimeSpan.FromMilliseconds(5),
                ScaleUpCooldown = TimeSpan.FromMilliseconds(5),
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(50),
                ScaleUpConsecutiveSamples = 1,
                ScaleObserver = scaleChanges.Enqueue
            });

        dispatcher.Start(handler);

        Assert.Empty(scaleChanges);

        try
        {
            dispatcher.Enqueue("hot", 0);
            await TestWait.UntilAsync(() => handler.HotStarted);

            for (var i = 1; i <= 3; i++)
            {
                dispatcher.Enqueue("hot", i);
            }

            dispatcher.Enqueue("cold", 0);
            await TestWait.UntilAsync(() => handler.ColdStarted);
            await TestWait.UntilAsync(() =>
            {
                var stats = dispatcher.GetStats();
                return stats.WorkerCount == 2 && stats.BusyWorkers == 2;
            });
            await TestWait.UntilAsync(() => scaleChanges.Count == 1);

            var scaleUp = Assert.Single(scaleChanges);
            Assert.Equal(1, scaleUp.PreviousWorkerCount);
            Assert.Equal(2, scaleUp.CurrentWorkerCount);
            Assert.True(scaleUp.IsScaleUp);
            Assert.Equal(2, scaleUp.Stats.WorkerCount);

            handler.ReleaseCold();

            await TestWait.UntilAsync(() =>
            {
                var stats = dispatcher.GetStats();
                return stats.WorkerCount == 1 &&
                    stats.BusyWorkers == 1 &&
                    stats.QueuedWorkItems == 0 &&
                    stats.PendingMessages > 0;
            });
            await TestWait.UntilAsync(() => scaleChanges.Count == 2);

            var scaleDown = scaleChanges.ToArray()[1];
            Assert.Equal(2, scaleDown.PreviousWorkerCount);
            Assert.Equal(1, scaleDown.CurrentWorkerCount);
            Assert.False(scaleDown.IsScaleUp);
            Assert.Equal(1, scaleDown.Stats.WorkerCount);
            Assert.Equal(0, scaleDown.Stats.QueuedWorkItems);
            Assert.True(scaleDown.Stats.PendingMessages > 0);
        }
        finally
        {
            handler.ReleaseAll();
            await dispatcher.CompleteAsync();
        }

        Assert.Equal(2, scaleChanges.Count);
        Assert.Equal(0, dispatcher.GetStats().PendingMessages);
    }

    [Fact]
    public async Task EnqueueBeforeStartThrows()
    {
        await using var dispatcher = new KeyedOrderedDispatcher<string, int>();

        Assert.Throws<InvalidOperationException>(() => dispatcher.Enqueue("a", 1));
    }

    private sealed class BlockingKeyedHandler : IKeyedMessageHandler<string, int>, IDisposable
    {
        private readonly ManualResetEventSlim _firstStarted = new();
        private readonly ManualResetEventSlim _release = new();

        public bool FirstStarted => _firstStarted.IsSet;

        public void Handle(string key, int message, CancellationToken cancellationToken)
        {
            _firstStarted.Set();
            _release.Wait(cancellationToken);
        }

        public void Release() => _release.Set();

        public void Dispose()
        {
            _firstStarted.Dispose();
            _release.Dispose();
        }
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

    private sealed class GatedKeyedHandler : IKeyedMessageHandler<string, int>, IDisposable
    {
        private readonly ManualResetEventSlim _hotStarted = new();
        private readonly ManualResetEventSlim _coldStarted = new();
        private readonly ManualResetEventSlim _releaseHot = new();
        private readonly ManualResetEventSlim _releaseCold = new();

        public bool HotStarted => _hotStarted.IsSet;

        public bool ColdStarted => _coldStarted.IsSet;

        public void Handle(string key, int message, CancellationToken cancellationToken)
        {
            if (key == "hot" && message == 0)
            {
                _hotStarted.Set();
                _releaseHot.Wait(cancellationToken);
            }
            else if (key == "cold")
            {
                _coldStarted.Set();
                _releaseCold.Wait(cancellationToken);
            }
        }

        public void ReleaseCold() => _releaseCold.Set();

        public void ReleaseAll()
        {
            _releaseHot.Set();
            _releaseCold.Set();
        }

        public void Dispose()
        {
            _hotStarted.Dispose();
            _coldStarted.Dispose();
            _releaseHot.Dispose();
            _releaseCold.Dispose();
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
