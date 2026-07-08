using System.Collections.Frozen;
using System.Threading.Channels;

namespace MessageDispatching;

public sealed class KeyedOrderedDispatcher<TKey, TMessage> : IAsyncDisposable
    where TKey : notnull
{
    private enum WorkerWaitResult
    {
        WorkAvailable,
        Completed,
        Retired
    }

    private sealed class KeyState
    {
        // CAS-based spinlock guarding the scheduling state transitions below. Queue writes stay
        // outside the gate (the channel is multi-writer-safe); see the ordering comment in
        // Enqueue. Single-reader is guaranteed by the Active flag.
        private int _gate;

        public readonly Channel<TMessage> Queue = Channel.CreateUnbounded<TMessage>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });

        // Count of queued messages not yet reserved by a worker. Mutated only inside Acquire():
        // the channel's own Reader.Count is unsupported, so scheduling uses this counter.
        public int UnreservedMessages;
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
    private readonly DispatcherOptions _options;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly CancellationTokenSource _scaleCts = new();
    private readonly object _lifetimeLock = new();
    private readonly object _workersLock = new();
    private readonly List<Task> _workers = new();
    private Task? _scaleController;

    private int _pendingMessages;
    private int _workerCount;
    private int _busyWorkers;
    private int _queuedWorkItemCount;
    private int _scaleUpCandidateSamples;
    private long _lastScaleUpTick;
    private IKeyedMessageHandler<TKey, TMessage>? _handler;
    private bool _started;
    private bool _accepting;
    private bool _completed;
    private bool _disposed;

    public KeyedOrderedDispatcher(DispatcherOptions? options = null)
    {
        _options = options ?? new DispatcherOptions();
        _options.Validate();

        // Unbounded so ScheduleKey never fails: at most one entry per active key can be queued,
        // and messages themselves live in the per-key queues, so this adds no unbounded memory.
        // Backpressure is out of scope by design (see HANDOFF: enqueue is non-blocking).
        _readyKeys = Channel.CreateUnbounded<TKey>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public void Start(IKeyedMessageHandler<TKey, TMessage> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(KeyedOrderedDispatcher<TKey, TMessage>));
            }

            if (_completed)
            {
                throw new InvalidOperationException("The dispatcher has already been completed.");
            }

            if (_started)
            {
                throw new InvalidOperationException("The dispatcher has already been started.");
            }

            Volatile.Write(ref _handler, handler);
            _started = true;
            Volatile.Write(ref _accepting, true);

            lock (_workersLock)
            {
                for (var i = 0; i < _options.Parallelism; i++)
                {
                    StartWorkerCore();
                }
            }

            if (_options.IsDynamicScalingEnabled)
            {
                _scaleController = ScaleControllerLoopAsync(_scaleCts.Token);
            }
        }
    }

    public DispatcherStats GetStats()
    {
        return new DispatcherStats(
            Volatile.Read(ref _pendingMessages),
            Volatile.Read(ref _states).Count,
            Volatile.Read(ref _workerCount),
            Volatile.Read(ref _busyWorkers),
            Volatile.Read(ref _queuedWorkItemCount),
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

            // Write-then-count ordering matters: the queue write stays outside the gate to keep
            // the critical section short, so UnreservedMessages only ever lags the queue, never
            // leads it. ProcessKey reserves at most UnreservedMessages entries, so a reserved
            // TryRead cannot come up empty. FIFO per key holds because concurrent Enqueue calls
            // for the same key are racing anyway — any interleaving of them is a valid order.
            if (!state.Queue.Writer.TryWrite(message))
            {
                throw new InvalidOperationException("Failed to enqueue the message into the key channel.");
            }

            using (state.Acquire())
            {
                state.UnreservedMessages++;

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
        var shouldCompleteWorkQueue = false;

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            _completed = true;
            Volatile.Write(ref _accepting, false);
            shouldCompleteWorkQueue = Volatile.Read(ref _pendingMessages) == 0;
        }

        if (shouldCompleteWorkQueue)
        {
            _scaleCts.Cancel();
            _readyKeys.Writer.TryComplete();
        }
    }

    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        Complete();
        await WaitForScaleControllerAsync(cancellationToken).ConfigureAwait(false);
        await Task.WhenAll(GetWorkerSnapshot()).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            _completed = true;
            Volatile.Write(ref _disposed, true);
            Volatile.Write(ref _accepting, false);
        }

        _readyKeys.Writer.TryComplete();
        _scaleCts.Cancel();
        _stopCts.Cancel();

        try
        {
            await WaitForScaleControllerAsync(default).ConfigureAwait(false);
            await Task.WhenAll(GetWorkerSnapshot()).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _stopCts.Dispose();
            _scaleCts.Dispose();
        }
    }

    private async Task ScaleControllerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(_options.ScaleInterval);

            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (Volatile.Read(ref _disposed) ||
                    (!Volatile.Read(ref _accepting) && Volatile.Read(ref _pendingMessages) == 0))
                {
                    return;
                }

                SampleScaleUp();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        var counted = true;

        try
        {
            while (true)
            {
                while (_readyKeys.Reader.TryRead(out var key))
                {
                    Interlocked.Decrement(ref _queuedWorkItemCount);
                    Interlocked.Increment(ref _busyWorkers);

                    try
                    {
                        ProcessKey(key, cancellationToken);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _busyWorkers);
                    }
                }

                var waitResult = await WaitForWorkOrRetireAsync(cancellationToken).ConfigureAwait(false);
                if (waitResult == WorkerWaitResult.WorkAvailable)
                {
                    continue;
                }

                if (waitResult == WorkerWaitResult.Retired)
                {
                    counted = false;
                }

                return;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            if (counted)
            {
                Interlocked.Decrement(ref _workerCount);
            }
        }
    }

    private async ValueTask<WorkerWaitResult> WaitForWorkOrRetireAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (!_options.IsDynamicScalingEnabled ||
                Volatile.Read(ref _workerCount) <= _options.Parallelism)
            {
                return await _readyKeys.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false)
                    ? WorkerWaitResult.WorkAvailable
                    : WorkerWaitResult.Completed;
            }

            using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            idleCts.CancelAfter(_options.ScaleDownIdleDuration);

            try
            {
                return await _readyKeys.Reader.WaitToReadAsync(idleCts.Token).ConfigureAwait(false)
                    ? WorkerWaitResult.WorkAvailable
                    : WorkerWaitResult.Completed;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                if (TryRetireIdleWorker())
                {
                    return WorkerWaitResult.Retired;
                }
            }
        }
    }

    private void ProcessKey(TKey key, CancellationToken cancellationToken)
    {
        if (!Volatile.Read(ref _states).TryGetValue(key, out var state))
        {
            return;
        }

        var processedMessages = 0;
        var reservedMessages = 0;

        using (state.Acquire())
        {
            reservedMessages = Math.Min(state.UnreservedMessages, _options.KeyBatchSize);
            state.UnreservedMessages -= reservedMessages;
        }

        // Drain the reserved batch outside the lock. A single consumer is guaranteed by the
        // Active flag, so the SPSC queue stays valid.
        while (processedMessages < reservedMessages && state.Queue.Reader.TryRead(out var message))
        {
            try
            {
                var handler = Volatile.Read(ref _handler) ??
                    throw new InvalidOperationException("The dispatcher has not been started.");
                handler.Handle(key, message, cancellationToken);
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

            processedMessages++;
        }

        var shouldReschedule = false;

        using (state.Acquire())
        {
            if (state.UnreservedMessages > 0)
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
        Interlocked.Increment(ref _queuedWorkItemCount);

        // The channel is unbounded, so TryWrite only fails after the writer is completed
        // (pending drained to zero after Complete, or dispose). Either way workers are done
        // with this key; just roll back the counter.
        if (!_readyKeys.Writer.TryWrite(key))
        {
            Interlocked.Decrement(ref _queuedWorkItemCount);
        }
    }

    private void SampleScaleUp()
    {
        var workerCount = Volatile.Read(ref _workerCount);
        var maxParallelism = _options.EffectiveMaxParallelism;

        if (workerCount >= maxParallelism ||
            !IsScaleUpCandidate(workerCount))
        {
            Interlocked.Exchange(ref _scaleUpCandidateSamples, 0);
            return;
        }

        if (Interlocked.Increment(ref _scaleUpCandidateSamples) < _options.ScaleUpConsecutiveSamples)
        {
            return;
        }

        var now = Environment.TickCount64;
        if (!IsScaleUpCooldownElapsed(now))
        {
            return;
        }

        lock (_workersLock)
        {
            PruneCompletedWorkersCore();
            workerCount = Volatile.Read(ref _workerCount);

            if (Volatile.Read(ref _disposed) ||
                workerCount >= maxParallelism ||
                !IsScaleUpCandidate(workerCount) ||
                !IsScaleUpCooldownElapsed(now))
            {
                return;
            }

            StartWorkerCore();
            Volatile.Write(ref _lastScaleUpTick, now);
            Interlocked.Exchange(ref _scaleUpCandidateSamples, 0);
        }
    }

    private bool IsScaleUpCandidate(int workerCount)
    {
        if (workerCount <= 0)
        {
            return false;
        }

        return Volatile.Read(ref _busyWorkers) >= workerCount &&
            Volatile.Read(ref _queuedWorkItemCount) >= _options.ScaleUpQueuedWorkItemsThreshold &&
            Volatile.Read(ref _pendingMessages) >=
                workerCount * _options.ScaleUpMessagesPerWorkerThreshold;
    }

    private bool IsScaleUpCooldownElapsed(long now)
    {
        var lastScaleUpTick = Volatile.Read(ref _lastScaleUpTick);
        if (lastScaleUpTick == 0)
        {
            return true;
        }

        return now - lastScaleUpTick >= _options.ScaleUpCooldown.TotalMilliseconds;
    }

    private void StartWorkerCore()
    {
        PruneCompletedWorkersCore();

        Interlocked.Increment(ref _workerCount);
        // Task.Run, not a direct call: an async method runs synchronously until its first real
        // await, and the worker loop starts by draining the ready queue. Called directly from
        // SampleScaleUp it would execute handlers on the timer thread while holding
        // _workersLock, blocking scaling decisions and CompleteAsync/DisposeAsync.
        var worker = Task.Run(() => WorkerLoopAsync(_stopCts.Token));
        _workers.Add(worker);
    }

    private bool TryRetireIdleWorker()
    {
        lock (_workersLock)
        {
            if (Volatile.Read(ref _workerCount) <= _options.Parallelism ||
                Volatile.Read(ref _pendingMessages) != 0 ||
                Volatile.Read(ref _queuedWorkItemCount) != 0 ||
                Volatile.Read(ref _disposed))
            {
                return false;
            }

            Interlocked.Decrement(ref _workerCount);
            return true;
        }
    }

    private Task[] GetWorkerSnapshot()
    {
        lock (_workersLock)
        {
            PruneCompletedWorkersCore();
            return _workers.ToArray();
        }
    }

    private void PruneCompletedWorkersCore()
    {
        for (var i = _workers.Count - 1; i >= 0; i--)
        {
            if (_workers[i].IsCompleted)
            {
                _workers.RemoveAt(i);
            }
        }
    }

    private async Task WaitForScaleControllerAsync(CancellationToken cancellationToken)
    {
        if (_scaleController is not null)
        {
            await _scaleController.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void MarkMessageCompleted()
    {
        var remaining = Interlocked.Decrement(ref _pendingMessages);

        if (remaining == 0 &&
            !Volatile.Read(ref _accepting) &&
            !Volatile.Read(ref _disposed))
        {
            _scaleCts.Cancel();
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
            var handler = Volatile.Read(ref _handler) ??
                throw new InvalidOperationException("The dispatcher has not been started.");
            handler.HandleError(key, message, exception, cancellationToken);
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
