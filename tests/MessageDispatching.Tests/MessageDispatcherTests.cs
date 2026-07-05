using MessageDispatching;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class MessageDispatcherTests
{
    [Fact]
    public async Task EnqueuedInputsAreTransformedAndPublished()
    {
        var transformer = new PrefixTransformer();
        var subscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 2
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        for (var i = 1; i <= 10; i++)
        {
            dispatcher.Enqueue(i);
        }

        await dispatcher.CompleteAsync();

        Assert.Equal(
            Enumerable.Range(1, 10).Select(static i => $"parsed-{i}").OrderBy(static value => value),
            subscriber.Messages.OrderBy(static value => value));
    }

    [Fact]
    public async Task TransformerErrorIsReportedAndOutputIsNotPublished()
    {
        var transformer = new ThrowingTransformer(inputToThrow: 2);
        var subscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 1,
                ScaleInterval = TimeSpan.FromMilliseconds(1),
                ScaleUpCooldown = TimeSpan.Zero,
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(1),
                ScaleUpQueuedWorkItemsThreshold = 1,
                ScaleUpMessagesPerWorkerThreshold = 1,
                ScaleUpConsecutiveSamples = 1
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        dispatcher.Enqueue(1);
        dispatcher.Enqueue(2);
        dispatcher.Enqueue(3);

        await dispatcher.CompleteAsync();

        Assert.Equal(new[] { 2 }, transformer.Errors);
        Assert.Equal(new[] { "parsed-1", "parsed-3" }, subscriber.Messages.OrderBy(static value => value));
    }

    [Fact]
    public async Task SubscriberFailureDoesNotStopOtherSubscribers()
    {
        var transformer = new PrefixTransformer();
        var throwingSubscriber = new ThrowingSubscriber();
        var recordingSubscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 1
            });

        using var firstSubscription = dispatcher.Subscribe(throwingSubscriber);
        using var secondSubscription = dispatcher.Subscribe(recordingSubscriber);
        dispatcher.Start(transformer);

        dispatcher.Enqueue(1);

        await dispatcher.CompleteAsync();

        Assert.Equal(new[] { "parsed-1" }, throwingSubscriber.Errors);
        Assert.Equal(new[] { "parsed-1" }, recordingSubscriber.Messages);
    }

    [Fact]
    public async Task DisposedSubscriptionStopsReceivingPublishedMessages()
    {
        var transformer = new PrefixTransformer();
        var subscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 1
            });

        var subscription = dispatcher.Subscribe(subscriber);
        subscription.Dispose();
        dispatcher.Start(transformer);
        dispatcher.Enqueue(1);

        await dispatcher.CompleteAsync();

        Assert.Empty(subscriber.Messages);
    }

    [Fact]
    public async Task DynamicWorkersScaleUpAndBackDown()
    {
        var transformer = new DelayingTransformer(TimeSpan.FromMilliseconds(25));
        var subscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 1,
                MaxParallelism = 4,
                ScaleInterval = TimeSpan.FromMilliseconds(5),
                ScaleUpCooldown = TimeSpan.FromMilliseconds(5),
                ScaleDownIdleDuration = TimeSpan.FromMilliseconds(50),
                ScaleUpQueuedWorkItemsThreshold = 2,
                ScaleUpMessagesPerWorkerThreshold = 2,
                ScaleUpConsecutiveSamples = 1
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        for (var i = 0; i < 40; i++)
        {
            dispatcher.Enqueue(i);
        }

        await TestWait.UntilAsync(() => dispatcher.GetStats().WorkerCount > 1);
        await TestWait.UntilAsync(() => dispatcher.GetStats().PendingMessages == 0);
        await TestWait.UntilAsync(() => dispatcher.GetStats().WorkerCount == 1);

        Assert.True(transformer.MaxConcurrency > 1);
        Assert.Equal(40, subscriber.Messages.Count);

        await dispatcher.CompleteAsync();
    }

    [Fact]
    public async Task SingleWorkerConfigurationAllowsMultipleProducersAndOneConsumer()
    {
        var transformer = new DelayingTransformer(TimeSpan.FromMilliseconds(5));
        var subscriber = new RecordingSubscriber();
        await using var dispatcher = new MessageDispatcher<int, string>(
            new DispatcherOptions
            {
                Parallelism = 1
            });

        using var subscription = dispatcher.Subscribe(subscriber);
        dispatcher.Start(transformer);

        const int producerCount = 8;
        const int messagesPerProducer = 25;

        var producers = Enumerable.Range(0, producerCount)
            .Select(producer => Task.Run(() =>
            {
                for (var i = 0; i < messagesPerProducer; i++)
                {
                    dispatcher.Enqueue(producer * messagesPerProducer + i);
                }
            }))
            .ToArray();

        await Task.WhenAll(producers);

        Assert.Equal(1, dispatcher.GetStats().WorkerCount);

        for (var i = 0; i < 6; i++)
        {
            dispatcher.Enqueue(i);
        }

        await dispatcher.CompleteAsync();

        Assert.Equal(producerCount * messagesPerProducer + 6, subscriber.Messages.Count);
        Assert.Equal(1, transformer.MaxConcurrency);
    }

    private sealed class PrefixTransformer : IMessageTransformer<int, string>
    {
        public string Transform(int input, CancellationToken cancellationToken) => $"parsed-{input}";
    }

    private sealed class DelayingTransformer : IMessageTransformer<int, string>
    {
        private readonly TimeSpan _delay;
        private int _currentConcurrency;
        private int _maxConcurrency;

        public DelayingTransformer(TimeSpan delay) => _delay = delay;

        public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

        public string Transform(int input, CancellationToken cancellationToken)
        {
            var running = Interlocked.Increment(ref _currentConcurrency);
            RecordMaxConcurrency(running);

            try
            {
                Thread.Sleep(_delay);
                return $"parsed-{input}";
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

    private sealed class ThrowingTransformer : IMessageTransformer<int, string>
    {
        private readonly int _inputToThrow;
        private readonly object _gate = new();
        private readonly List<int> _errors = new();

        public ThrowingTransformer(int inputToThrow) => _inputToThrow = inputToThrow;

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

        public string Transform(int input, CancellationToken cancellationToken)
        {
            if (input == _inputToThrow)
            {
                throw new InvalidOperationException("Expected test failure.");
            }

            return $"parsed-{input}";
        }

        public void HandleError(int input, Exception exception, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _errors.Add(input);
            }
        }
    }

    private sealed class RecordingSubscriber : IMessageSubscriber<string>
    {
        private readonly object _gate = new();
        private readonly List<string> _messages = new();

        public IReadOnlyList<string> Messages
        {
            get
            {
                lock (_gate)
                {
                    return _messages.ToArray();
                }
            }
        }

        public void Handle(string message, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _messages.Add(message);
            }
        }
    }

    private sealed class ThrowingSubscriber : IMessageSubscriber<string>
    {
        private readonly object _gate = new();
        private readonly List<string> _errors = new();

        public IReadOnlyList<string> Errors
        {
            get
            {
                lock (_gate)
                {
                    return _errors.ToArray();
                }
            }
        }

        public void Handle(string message, CancellationToken cancellationToken)
            => throw new InvalidOperationException("Expected subscriber failure.");

        public void HandleError(string message, Exception exception, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _errors.Add(message);
            }
        }
    }
}
