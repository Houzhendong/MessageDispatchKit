using System.Collections.Frozen;
using System.Threading.Channels;

namespace MessageDispatching;

public sealed class KeyedOrderedDispatcher<TKey, TMessage> : IAsyncDisposable
    where TKey : notnull
{
    private sealed class KeyState
    {
        // CAS-based spinlock guarding the scheduling state transitions below. The caller is
        // expected to preserve single-writer-per-key semantics for the per-key SPSC queue.
        private int _gate;

        public readonly Channel<TMessage> Queue = Channel.CreateUnbounded<TMessage>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false
            });

        // Count of queued messages not yet reserved by a worker. Mutated only inside Acquire():
        // the SPSC channel's own Reader.Count is unsupported, so scheduling uses this counter.
        public int Pending;
        public bool Active;

        public Releaser Acquire()
        {
            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref _gate, 1, 0) != 0)
            {
                spinner.SpinOnce();
            }

            return new Releaser(this);
        }

        public readonly struct Releaser : IDisposable
        {
            private readonly KeyState _state;

            internal Releaser(KeyState state) => _state = state;

            public void Dispose() => Volatile.Write(ref _state._gate, 0);
        }
    }

    // Published as copy-on-write frozen snapshots: hot-path reads are lock-free once a key exists,
    // while the rare first enqueue for a new key takes the lock and publishes a rebuilt map.
    private readonly object _statesLock = new();
    private FrozenDictionary<TKey, KeyState> _states = FrozenDictionary<TKey, KeyState>.Empty;
    private readonly Channel<TKey> _readyKeys;
    private readonly IKeyedMessageHandler<TKey, TMessage> _handler;
    private readonly KeyedOrderedDispatcherOptions _options;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly object _lifetimeLock = new();
    private readonly Task[] _workers;
    private readonly Task _completion;

    private int _pendingMessages;
    private bool _accepting = true;
    private bool _disposed;

    public KeyedOrderedDispatcher(
        IKeyedMessageHandler<TKey, TMessage> handler,
        KeyedOrderedDispatcherOptions? options = null)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _options = options ?? new KeyedOrderedDispatcherOptions();
        _options.Validate();

        _readyKeys = Channel.CreateUnbounded<TKey>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });

        _workers = new Task[_options.Parallelism];
        for (var i = 0; i < _workers.Length; i++)
        {
            _workers[i] = Task.Run(() => WorkerLoopAsync(_stopCts.Token));
        }

        _completion = Task.WhenAll(_workers);
    }

    public KeyedOrderedDispatcherStats GetStats()
    {
        return new KeyedOrderedDispatcherStats(
            Volatile.Read(ref _pendingMessages),
            Volatile.Read(ref _states).Count,
            Volatile.Read(ref _accepting) && !Volatile.Read(ref _disposed));
    }

    public void Enqueue(TKey key, TMessage message, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Interlocked.Increment(ref _pendingMessages);

        try
        {
            if (!Volatile.Read(ref _accepting) || Volatile.Read(ref _disposed))
            {
                throw new InvalidOperationException("The dispatcher is not accepting new messages.");
            }

            cancellationToken.ThrowIfCancellationRequested();

            var state = GetOrCreateState(key);
            var shouldSchedule = false;

            if (!state.Queue.Writer.TryWrite(message))
            {
                throw new InvalidOperationException("Failed to enqueue the message into the key channel.");
            }

            using (state.Acquire())
            {
                state.Pending++;

                if (!state.Active)
                {
                    state.Active = true;
                    shouldSchedule = true;
                }
            }

            if (shouldSchedule)
            {
                ScheduleKey(key);
            }

            return;
        }
        catch
        {
            // The message was counted, but enqueue/scheduling failed; roll the in-flight count back.
            MarkMessageCompleted();
            throw;
        }
    }

    public void Complete()
    {
        var shouldCompleteReadyKeys = false;

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            Volatile.Write(ref _accepting, false);
            shouldCompleteReadyKeys = Volatile.Read(ref _pendingMessages) == 0;
        }

        if (shouldCompleteReadyKeys)
        {
            _readyKeys.Writer.TryComplete();
        }
    }

    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        Complete();
        await _completion.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            Volatile.Write(ref _disposed, true);
            Volatile.Write(ref _accepting, false);
        }

        _readyKeys.Writer.TryComplete();
        _stopCts.Cancel();

        try
        {
            await _completion.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _stopCts.Dispose();
        }
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var key in _readyKeys.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                ProcessKey(key, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private void ProcessKey(TKey key, CancellationToken cancellationToken)
    {
        if (!Volatile.Read(ref _states).TryGetValue(key, out var state))
        {
            return;
        }

        var processed = 0;
        var reserved = 0;

        using (state.Acquire())
        {
            reserved = Math.Min(state.Pending, _options.BatchSize);
            state.Pending -= reserved;
        }

        // Drain the reserved batch outside the lock. A single consumer is guaranteed by the
        // Active flag, so the SPSC queue stays valid.
        while (processed < reserved && state.Queue.Reader.TryRead(out var message))
        {
            try
            {
                _handler.Handle(key, message, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                TryReportError(key, message, ex, cancellationToken);
            }
            finally
            {
                MarkMessageCompleted();
            }

            processed++;
        }

        var shouldReschedule = false;

        using (state.Acquire())
        {
            if (state.Pending > 0)
            {
                shouldReschedule = true;
            }
            else
            {
                state.Active = false;
            }
        }

        if (shouldReschedule)
        {
            ScheduleKey(key);
        }
    }

    private KeyState GetOrCreateState(TKey key)
    {
        var snapshot = Volatile.Read(ref _states);
        if (snapshot.TryGetValue(key, out var state))
        {
            return state;
        }

        lock (_statesLock)
        {
            snapshot = _states;
            if (snapshot.TryGetValue(key, out state))
            {
                return state;
            }

            state = new KeyState();

            var updated = new Dictionary<TKey, KeyState>(snapshot.Count + 1)
            {
                [key] = state
            };

            foreach (var pair in snapshot)
            {
                updated.Add(pair.Key, pair.Value);
            }

            Volatile.Write(ref _states, updated.ToFrozenDictionary());
            return state;
        }
    }

    private void ScheduleKey(TKey key)
    {
        if (!_readyKeys.Writer.TryWrite(key))
        {
            throw new InvalidOperationException("The dispatcher cannot schedule work because it is completed.");
        }
    }

    private void MarkMessageCompleted()
    {
        var remaining = Interlocked.Decrement(ref _pendingMessages);

        if (remaining == 0 &&
            !Volatile.Read(ref _accepting) &&
            !Volatile.Read(ref _disposed))
        {
            _readyKeys.Writer.TryComplete();
        }
    }

    private void TryReportError(
        TKey key,
        TMessage message,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            _handler.HandleError(key, message, exception, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // The dispatcher keeps processing; logging failures belong in the supplied error handler.
        }
    }
}
