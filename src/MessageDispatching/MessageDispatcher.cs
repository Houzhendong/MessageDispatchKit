using System.Threading.Channels;

namespace MessageDispatching;

public sealed class MessageDispatcher<TInput, TOutput> : IAsyncDisposable
{
    private enum WorkerWaitResult
    {
        WorkAvailable,
        Completed,
        Retired
    }

    private sealed class Subscription : IDisposable
    {
        private readonly MessageDispatcher<TInput, TOutput> _dispatcher;
        private IMessageSubscriber<TOutput>? _subscriber;

        public Subscription(
            MessageDispatcher<TInput, TOutput> dispatcher,
            IMessageSubscriber<TOutput> subscriber)
        {
            _dispatcher = dispatcher;
            _subscriber = subscriber;
        }

        public void Dispose()
        {
            var subscriber = Interlocked.Exchange(ref _subscriber, null);
            if (subscriber is not null)
            {
                _dispatcher.Unsubscribe(subscriber);
            }
        }
    }

    private readonly Channel<TInput> _queue;
    private readonly DispatcherOptions _options;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly CancellationTokenSource _scaleCts = new();
    private readonly object _lifetimeLock = new();
    private readonly object _workersLock = new();
    private readonly object _subscribersLock = new();
    private readonly List<Task> _workers = new();
    private readonly bool _singleWorkerMode;
    private readonly bool _dynamicScalingEnabled;
    private Task? _scaleController;

    private int _pendingMessages;
    private int _queuedWorkItemCount;
    private int _workerCount;
    private int _busyWorkers;
    private int _scaleUpCandidateSamples;
    private long _lastScaleUpTick;
    private IMessageTransformer<TInput, TOutput>? _transformer;
    private IMessageSubscriber<TOutput>[] _subscribers = [];
    private bool _started;
    private bool _accepting;
    private bool _completed;
    private bool _disposed;

    public MessageDispatcher(DispatcherOptions? options = null)
    {
        _options = options ?? new DispatcherOptions();
        _options.Validate();

        _singleWorkerMode = _options.EffectiveMaxParallelism == 1;
        _dynamicScalingEnabled = _options.IsDynamicScalingEnabled && !_singleWorkerMode;

        _queue = Channel.CreateUnbounded<TInput>(
            new UnboundedChannelOptions
            {
                SingleReader = _singleWorkerMode,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public IDisposable Subscribe(IMessageSubscriber<TOutput> subscriber)
    {
        ArgumentNullException.ThrowIfNull(subscriber);

        lock (_subscribersLock)
        {
            var snapshot = Volatile.Read(ref _subscribers);
            var updated = new IMessageSubscriber<TOutput>[snapshot.Length + 1];
            Array.Copy(snapshot, updated, snapshot.Length);
            updated[^1] = subscriber;
            Volatile.Write(ref _subscribers, updated);
        }

        return new Subscription(this, subscriber);
    }

    public void Start(IMessageTransformer<TInput, TOutput> transformer)
    {
        ArgumentNullException.ThrowIfNull(transformer);

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MessageDispatcher<TInput, TOutput>));
            }

            if (_completed)
            {
                throw new InvalidOperationException("The dispatcher has already been completed.");
            }

            if (_started)
            {
                throw new InvalidOperationException("The dispatcher has already been started.");
            }

            Volatile.Write(ref _transformer, transformer);
            _started = true;
            Volatile.Write(ref _accepting, true);

            lock (_workersLock)
            {
                for (var i = 0; i < _options.Parallelism; i++)
                {
                    StartWorkerCore();
                }
            }

            if (_dynamicScalingEnabled)
            {
                _scaleController = ScaleControllerLoopAsync(_scaleCts.Token);
            }
        }
    }

    public DispatcherStats GetStats()
    {
        return new DispatcherStats(
            Volatile.Read(ref _pendingMessages),
            0,
            Volatile.Read(ref _workerCount),
            Volatile.Read(ref _busyWorkers),
            Volatile.Read(ref _queuedWorkItemCount),
            Volatile.Read(ref _accepting) && !Volatile.Read(ref _disposed));
    }

    public void Enqueue(TInput input, CancellationToken cancellationToken = default)
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

            Interlocked.Increment(ref _queuedWorkItemCount);

            if (!_queue.Writer.TryWrite(input))
            {
                Interlocked.Decrement(ref _queuedWorkItemCount);
                throw new InvalidOperationException("Failed to enqueue the message.");
            }
        }
        catch
        {
            MarkMessageCompleted();
            throw;
        }
    }

    public void Complete()
    {
        var shouldCompleteQueue = false;

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            _completed = true;
            Volatile.Write(ref _accepting, false);
            shouldCompleteQueue = Volatile.Read(ref _pendingMessages) == 0;
        }

        if (shouldCompleteQueue)
        {
            _scaleCts.Cancel();
            _queue.Writer.TryComplete();
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

        _queue.Writer.TryComplete();
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
                while (_queue.Reader.TryRead(out var input))
                {
                    ProcessQueuedInput(input, cancellationToken);
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

    private async Task SingleWorkerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _queue.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_queue.Reader.TryRead(out var input))
                {
                    ProcessQueuedInput(input, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            Interlocked.Decrement(ref _workerCount);
        }
    }

    private async ValueTask<WorkerWaitResult> WaitForWorkOrRetireAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (!_dynamicScalingEnabled ||
                Volatile.Read(ref _workerCount) <= _options.Parallelism)
            {
                return await _queue.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false)
                    ? WorkerWaitResult.WorkAvailable
                    : WorkerWaitResult.Completed;
            }

            using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            idleCts.CancelAfter(_options.ScaleDownIdleDuration);

            try
            {
                return await _queue.Reader.WaitToReadAsync(idleCts.Token).ConfigureAwait(false)
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

    private void ProcessMessage(TInput input, CancellationToken cancellationToken)
    {
        try
        {
            var transformer = Volatile.Read(ref _transformer) ??
                throw new InvalidOperationException("The dispatcher has not been started.");
            var output = transformer.Transform(input, cancellationToken);
            Publish(output, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            TryReportTransformError(input, ex, cancellationToken);
        }
        finally
        {
            MarkMessageCompleted();
        }
    }

    private void ProcessQueuedInput(TInput input, CancellationToken cancellationToken)
    {
        Interlocked.Decrement(ref _queuedWorkItemCount);
        Interlocked.Increment(ref _busyWorkers);

        try
        {
            ProcessMessage(input, cancellationToken);
        }
        finally
        {
            Interlocked.Decrement(ref _busyWorkers);
        }
    }

    private void SampleScaleUp()
    {
        if (!_dynamicScalingEnabled)
        {
            return;
        }

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
        // await, and the worker loop starts by draining the queue. Called directly from
        // SampleScaleUp it would execute transformers on the timer thread while holding
        // _workersLock, blocking scaling decisions and CompleteAsync/DisposeAsync.
        var worker = _singleWorkerMode
            ? Task.Run(() => SingleWorkerLoopAsync(_stopCts.Token))
            : Task.Run(() => WorkerLoopAsync(_stopCts.Token));
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
            _queue.Writer.TryComplete();
        }
    }

    private void Publish(TOutput output, CancellationToken cancellationToken)
    {
        var subscribers = Volatile.Read(ref _subscribers);
        foreach (var subscriber in subscribers)
        {
            try
            {
                subscriber.Handle(output, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                TryReportSubscriberError(subscriber, output, ex, cancellationToken);
            }
        }
    }

    private void TryReportTransformError(
        TInput input,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            var transformer = Volatile.Read(ref _transformer) ??
                throw new InvalidOperationException("The dispatcher has not been started.");
            transformer.HandleError(input, exception, cancellationToken);
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

    private static void TryReportSubscriberError(
        IMessageSubscriber<TOutput> subscriber,
        TOutput output,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            subscriber.HandleError(output, exception, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Subscriber error handlers should own their own logging/retry failures.
        }
    }

    private void Unsubscribe(IMessageSubscriber<TOutput> subscriber)
    {
        lock (_subscribersLock)
        {
            var snapshot = Volatile.Read(ref _subscribers);
            var index = Array.IndexOf(snapshot, subscriber);
            if (index < 0)
            {
                return;
            }

            if (snapshot.Length == 1)
            {
                Volatile.Write(ref _subscribers, []);
                return;
            }

            var updated = new IMessageSubscriber<TOutput>[snapshot.Length - 1];
            if (index > 0)
            {
                Array.Copy(snapshot, 0, updated, 0, index);
            }

            if (index < snapshot.Length - 1)
            {
                Array.Copy(snapshot, index + 1, updated, index, snapshot.Length - index - 1);
            }

            Volatile.Write(ref _subscribers, updated);
        }
    }
}
