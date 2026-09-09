using System.Collections.Frozen;
using System.Diagnostics;
using System.Threading.Channels;

namespace MessageDispatching;

public sealed class KeyedOrderedDispatcher<TKey, TMessage> : IAsyncDisposable
    where TKey : notnull
{
    private sealed class WorkerRegistration : IDisposable
    {
        public readonly CancellationTokenSource RetirementCts = new();
        public Task WorkerTask { get; set; } = Task.CompletedTask;
        public bool IsWaiting;
        public bool RetirementRequested;

        public void Dispose() => RetirementCts.Dispose();
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
    private readonly Action<DispatcherScaleChange>? _scaleObserver;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly CancellationTokenSource _scaleCts = new();
    private readonly object _lifetimeLock = new();
    private readonly object _workersLock = new();
    private readonly List<WorkerRegistration> _workers = new();
    private readonly DynamicScalingPolicy? _scalingPolicy;
    private Task? _scaleController;

    private long _pendingMessages;
    private long _completedMessages;
    private long _scaleUpCount;
    private long _scaleDownCount;
    private long _probeAcceptCount;
    private long _probeRejectCount;
    private int _workerCount;
    private int _desiredWorkerCount;
    private int _busyWorkers;
    private int _readyKeyCount;
    private double _throughput;
    private double _smoothedThroughput;
    private double _lastProbeGain = double.NaN;
    private IKeyedMessageHandler<TKey, TMessage>? _handler;
    private bool _started;
    private bool _accepting;
    private bool _completed;
    private bool _disposed;

    public KeyedOrderedDispatcher(DispatcherOptions? options = null)
    {
        _options = options ?? new DispatcherOptions();
        _options.Validate();
        _scaleObserver = _options.ScaleObserver;
        _scalingPolicy = _options.IsDynamicScalingEnabled
            ? new DynamicScalingPolicy(_options)
            : null;

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
            Volatile.Write(ref _desiredWorkerCount, _options.Parallelism);

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

    public KeyedDispatcherStats GetStats()
    {
        var workerCount = Volatile.Read(ref _workerCount);
        var busyWorkers = Volatile.Read(ref _busyWorkers);
        var readyKeyCount = Volatile.Read(ref _readyKeyCount);
        var lastProbeGain = Volatile.Read(ref _lastProbeGain);

        return new KeyedDispatcherStats(
            Interlocked.Read(ref _pendingMessages),
            Interlocked.Read(ref _completedMessages),
            Volatile.Read(ref _states).Count,
            workerCount,
            Volatile.Read(ref _desiredWorkerCount),
            busyWorkers,
            readyKeyCount,
            Volatile.Read(ref _throughput),
            Volatile.Read(ref _smoothedThroughput),
            IsSaturated(workerCount, busyWorkers, readyKeyCount),
            Interlocked.Read(ref _scaleUpCount),
            Interlocked.Read(ref _scaleDownCount),
            Interlocked.Read(ref _probeAcceptCount),
            Interlocked.Read(ref _probeRejectCount),
            double.IsNaN(lastProbeGain) ? null : lastProbeGain,
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
        }
        catch
        {
            RollbackPendingMessage();
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
            shouldCompleteWorkQueue = Interlocked.Read(ref _pendingMessages) == 0;
        }

        if (shouldCompleteWorkQueue)
        {
            FinishGracefulDrain();
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

            lock (_workersLock)
            {
                Volatile.Write(ref _desiredWorkerCount, 0);
            }
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
            using var timer = new PeriodicTimer(_options.DynamicScaling.SampleInterval);

            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (Volatile.Read(ref _disposed) ||
                    (!Volatile.Read(ref _accepting) && Interlocked.Read(ref _pendingMessages) == 0))
                {
                    return;
                }

                SampleScale();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private void SampleScale()
    {
        List<WorkerRegistration>? retirements = null;
        var previousWorkerCount = 0;
        var currentWorkerCount = 0;
        var scalingPolicy = _scalingPolicy ??
            throw new InvalidOperationException("Dynamic scaling is not enabled.");

        lock (_workersLock)
        {
            PruneCompletedWorkersCore();

            if (IsTerminalDrain())
            {
                Volatile.Write(ref _desiredWorkerCount, 0);
                return;
            }

            var result = scalingPolicy.Observe(
                new ScalingSnapshot(
                    Volatile.Read(ref _workerCount),
                    Volatile.Read(ref _desiredWorkerCount),
                    Volatile.Read(ref _busyWorkers),
                    Volatile.Read(ref _readyKeyCount),
                    Interlocked.Read(ref _completedMessages),
                    Stopwatch.GetTimestamp()));

            PublishScalingResult(result);
            Volatile.Write(ref _desiredWorkerCount, result.DesiredWorkerCount);

            var workerCount = Volatile.Read(ref _workerCount);
            if (workerCount < result.DesiredWorkerCount)
            {
                previousWorkerCount = workerCount;
                while (Volatile.Read(ref _workerCount) < result.DesiredWorkerCount)
                {
                    StartWorkerCore();
                }

                currentWorkerCount = Volatile.Read(ref _workerCount);
                Interlocked.Add(ref _scaleUpCount, currentWorkerCount - previousWorkerCount);
            }
            else if (workerCount > result.DesiredWorkerCount)
            {
                retirements = RequestWaitingRetirementsCore();
            }
        }

        CancelRetirements(retirements);

        if (currentWorkerCount > previousWorkerCount && !Volatile.Read(ref _disposed))
        {
            NotifyScaleCompleted(previousWorkerCount, currentWorkerCount);
        }
    }

    private void PublishScalingResult(ScalingResult result)
    {
        Volatile.Write(ref _throughput, result.Throughput);
        Volatile.Write(ref _smoothedThroughput, result.SmoothedThroughput);

        if (result.Reason == ScalingReason.ProbeAccepted)
        {
            Interlocked.Increment(ref _probeAcceptCount);
            Volatile.Write(ref _lastProbeGain, result.ProbeGain ?? double.NaN);
        }
        else if (result.Reason == ScalingReason.ProbeRejected)
        {
            Interlocked.Increment(ref _probeRejectCount);
            Volatile.Write(ref _lastProbeGain, result.ProbeGain ?? double.NaN);
        }
    }

    private async Task WorkerLoopAsync(
        WorkerRegistration registration,
        CancellationToken cancellationToken)
    {
        using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            registration.RetirementCts.Token);

        try
        {
            while (true)
            {
                if (TryClaimRetirement(registration))
                {
                    return;
                }

                while (_readyKeys.Reader.TryRead(out var key))
                {
                    Interlocked.Decrement(ref _readyKeyCount);
                    Interlocked.Increment(ref _busyWorkers);
                    var shouldReschedule = false;

                    try
                    {
                        shouldReschedule = ProcessKey(key, cancellationToken);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _busyWorkers);
                    }

                    if (shouldReschedule)
                    {
                        ScheduleKey(key);
                    }

                    if (TryClaimRetirement(registration))
                    {
                        return;
                    }
                }

                if (TryClaimRetirement(registration))
                {
                    return;
                }

                SetWorkerWaiting(registration, true);
                try
                {
                    if (!await _readyKeys.Reader.WaitToReadAsync(waitCts.Token).ConfigureAwait(false))
                    {
                        return;
                    }
                }
                finally
                {
                    SetWorkerWaiting(registration, false);
                }
            }
        }
        catch (OperationCanceledException) when (
            cancellationToken.IsCancellationRequested ||
            registration.RetirementCts.IsCancellationRequested)
        {
        }
        finally
        {
            var dynamicRetirement = Volatile.Read(ref registration.RetirementRequested);
            int previousWorkerCount;
            int currentWorkerCount;

            lock (_workersLock)
            {
                previousWorkerCount = Volatile.Read(ref _workerCount);
                currentWorkerCount = Interlocked.Decrement(ref _workerCount);
            }

            if (dynamicRetirement && !IsTerminalDrain())
            {
                Interlocked.Increment(ref _scaleDownCount);
                NotifyScaleCompleted(previousWorkerCount, currentWorkerCount);
            }
        }
    }

    private bool TryClaimRetirement(WorkerRegistration registration)
    {
        if (Volatile.Read(ref registration.RetirementRequested))
        {
            return true;
        }

        if (IsTerminalDrain())
        {
            return false;
        }

        lock (_workersLock)
        {
            if (registration.RetirementRequested)
            {
                return true;
            }

            var requestedRetirements = CountRequestedRetirementsCore();
            if (Volatile.Read(ref _workerCount) - requestedRetirements <=
                Volatile.Read(ref _desiredWorkerCount))
            {
                return false;
            }

            Volatile.Write(ref registration.RetirementRequested, true);
            return true;
        }
    }

    private List<WorkerRegistration>? RequestWaitingRetirementsCore()
    {
        if (IsTerminalDrain())
        {
            return null;
        }

        var remaining = Volatile.Read(ref _workerCount) -
            CountRequestedRetirementsCore() -
            Volatile.Read(ref _desiredWorkerCount);
        if (remaining <= 0)
        {
            return null;
        }

        List<WorkerRegistration>? retirements = null;
        foreach (var worker in _workers)
        {
            if (remaining == 0)
            {
                break;
            }

            if (!worker.IsWaiting || worker.RetirementRequested || worker.WorkerTask.IsCompleted)
            {
                continue;
            }

            Volatile.Write(ref worker.RetirementRequested, true);
            (retirements ??= []).Add(worker);
            remaining--;
        }

        return retirements;
    }

    private int CountRequestedRetirementsCore()
    {
        var count = 0;
        foreach (var worker in _workers)
        {
            if (worker.RetirementRequested && !worker.WorkerTask.IsCompleted)
            {
                count++;
            }
        }

        return count;
    }

    private static void CancelRetirements(List<WorkerRegistration>? retirements)
    {
        if (retirements is null)
        {
            return;
        }

        foreach (var retirement in retirements)
        {
            try
            {
                retirement.RetirementCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The worker already observed the retirement request and was pruned.
            }
        }
    }

    private void SetWorkerWaiting(WorkerRegistration registration, bool isWaiting)
    {
        lock (_workersLock)
        {
            registration.IsWaiting = isWaiting;
        }
    }

    private bool ProcessKey(TKey key, CancellationToken cancellationToken)
    {
        if (!Volatile.Read(ref _states).TryGetValue(key, out var state))
        {
            return false;
        }

        var processedMessages = 0;
        var reservedMessages = 0;

        using (state.Acquire())
        {
            reservedMessages = Math.Min(state.UnreservedMessages, _options.KeyBatchSize);
            state.UnreservedMessages -= reservedMessages;
        }

        // Drain the reserved batch outside the lock. The Active flag guarantees one consumer
        // even though concurrent producers may enqueue to the per-key channel.
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
                MarkProcessingAttemptCompleted();
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

        return shouldReschedule;
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
        Interlocked.Increment(ref _readyKeyCount);

        // The channel is unbounded, so TryWrite only fails after the writer is completed
        // (pending drained to zero after Complete, or dispose). Either way workers are done
        // with this key; just roll back the counter.
        if (!_readyKeys.Writer.TryWrite(key))
        {
            Interlocked.Decrement(ref _readyKeyCount);
        }
    }

    private void NotifyScaleCompleted(int previousWorkerCount, int currentWorkerCount)
    {
        if (_scaleObserver is null)
        {
            return;
        }

        try
        {
            var change = new DispatcherScaleChange(
                previousWorkerCount,
                currentWorkerCount,
                GetScalingStats());
            _scaleObserver(change);
        }
        catch
        {
            // Scale observation must not affect dispatcher state or processing.
        }
    }

    private DispatcherScalingStats GetScalingStats()
    {
        var workerCount = Volatile.Read(ref _workerCount);
        var busyWorkers = Volatile.Read(ref _busyWorkers);
        var readyKeyCount = Volatile.Read(ref _readyKeyCount);
        var lastProbeGain = Volatile.Read(ref _lastProbeGain);

        return new DispatcherScalingStats(
            Interlocked.Read(ref _pendingMessages),
            Interlocked.Read(ref _completedMessages),
            workerCount,
            Volatile.Read(ref _desiredWorkerCount),
            busyWorkers,
            readyKeyCount,
            Volatile.Read(ref _throughput),
            Volatile.Read(ref _smoothedThroughput),
            IsSaturated(workerCount, busyWorkers, readyKeyCount),
            Interlocked.Read(ref _scaleUpCount),
            Interlocked.Read(ref _scaleDownCount),
            Interlocked.Read(ref _probeAcceptCount),
            Interlocked.Read(ref _probeRejectCount),
            double.IsNaN(lastProbeGain) ? null : lastProbeGain,
            Volatile.Read(ref _accepting) && !Volatile.Read(ref _disposed));
    }

    private int StartWorkerCore()
    {
        PruneCompletedWorkersCore();

        var registration = new WorkerRegistration();
        var workerCount = Interlocked.Increment(ref _workerCount);
        // Task.Run, not a direct call: an async method runs synchronously until its first real
        // await, and the worker loop starts by draining the ready queue. Called directly from
        // the scale controller it would execute handlers on the timer thread while holding
        // _workersLock, blocking scaling decisions and CompleteAsync/DisposeAsync.
        registration.WorkerTask = Task.Run(
            () => WorkerLoopAsync(registration, _stopCts.Token));
        _workers.Add(registration);
        return workerCount;
    }

    private Task[] GetWorkerSnapshot()
    {
        lock (_workersLock)
        {
            PruneCompletedWorkersCore();
            return _workers.Select(worker => worker.WorkerTask).ToArray();
        }
    }

    private void PruneCompletedWorkersCore()
    {
        for (var i = _workers.Count - 1; i >= 0; i--)
        {
            var worker = _workers[i];
            if (worker.WorkerTask.IsCompleted)
            {
                worker.Dispose();
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

    private void MarkProcessingAttemptCompleted()
    {
        Interlocked.Increment(ref _completedMessages);
        DecrementPendingMessage();
    }

    private void RollbackPendingMessage()
    {
        DecrementPendingMessage();
    }

    private void DecrementPendingMessage()
    {
        var remaining = Interlocked.Decrement(ref _pendingMessages);

        if (remaining == 0 &&
            Volatile.Read(ref _completed) &&
            !Volatile.Read(ref _accepting) &&
            !Volatile.Read(ref _disposed))
        {
            FinishGracefulDrain();
        }
    }

    private void FinishGracefulDrain()
    {
        Volatile.Write(ref _desiredWorkerCount, 0);
        _scaleCts.Cancel();
        _readyKeys.Writer.TryComplete();
    }

    private bool IsTerminalDrain()
    {
        return Volatile.Read(ref _disposed) ||
            (!Volatile.Read(ref _accepting) && Interlocked.Read(ref _pendingMessages) == 0);
    }

    private static bool IsSaturated(int workerCount, int busyWorkers, long readyWorkItemCount)
    {
        return workerCount > 0 && busyWorkers >= workerCount && readyWorkItemCount > 0;
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
