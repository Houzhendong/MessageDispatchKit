using System.Diagnostics;
using System.Threading.Channels;

namespace MessageDispatching;

public sealed class MessageDispatcher<TInput, TOutput> : IAsyncDisposable
{
    private sealed class WorkerRegistration : IDisposable
    {
        public readonly CancellationTokenSource RetirementCts = new();
        public Task WorkerTask { get; set; } = Task.CompletedTask;
        public bool IsWaiting;
        public bool RetirementRequested;

        public void Dispose() => RetirementCts.Dispose();
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
    private readonly Action<DispatcherScaleChange>? _scaleObserver;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly CancellationTokenSource _scaleCts = new();
    private readonly object _lifetimeLock = new();
    private readonly object _workersLock = new();
    private readonly object _subscribersLock = new();
    private readonly List<WorkerRegistration> _workers = new();
    private readonly DynamicScalingPolicy? _scalingPolicy;
    private readonly bool _singleWorkerMode;
    private readonly bool _dynamicScalingEnabled;
    private Task? _scaleController;

    private long _pendingMessages;
    private long _completedMessages;
    private long _queuedMessageCount;
    private long _scaleUpCount;
    private long _scaleDownCount;
    private long _probeAcceptCount;
    private long _probeRejectCount;
    private int _workerCount;
    private int _desiredWorkerCount;
    private int _busyWorkers;
    private double _throughput;
    private double _smoothedThroughput;
    private double _lastProbeGain = double.NaN;
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
        _scaleObserver = _options.ScaleObserver;

        _singleWorkerMode = _options.EffectiveMaxParallelism == 1;
        _dynamicScalingEnabled = _options.IsDynamicScalingEnabled && !_singleWorkerMode;
        _scalingPolicy = _dynamicScalingEnabled
            ? new DynamicScalingPolicy(_options)
            : null;

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
            Volatile.Write(ref _desiredWorkerCount, _options.Parallelism);

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
        var workerCount = Volatile.Read(ref _workerCount);
        var busyWorkers = Volatile.Read(ref _busyWorkers);
        var queuedMessageCount = Interlocked.Read(ref _queuedMessageCount);
        var lastProbeGain = Volatile.Read(ref _lastProbeGain);

        return new DispatcherStats(
            Interlocked.Read(ref _pendingMessages),
            Interlocked.Read(ref _completedMessages),
            workerCount,
            Volatile.Read(ref _desiredWorkerCount),
            busyWorkers,
            queuedMessageCount,
            Volatile.Read(ref _throughput),
            Volatile.Read(ref _smoothedThroughput),
            IsSaturated(workerCount, busyWorkers, queuedMessageCount),
            Interlocked.Read(ref _scaleUpCount),
            Interlocked.Read(ref _scaleDownCount),
            Interlocked.Read(ref _probeAcceptCount),
            Interlocked.Read(ref _probeRejectCount),
            double.IsNaN(lastProbeGain) ? null : lastProbeGain,
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

            Interlocked.Increment(ref _queuedMessageCount);

            if (!_queue.Writer.TryWrite(input))
            {
                Interlocked.Decrement(ref _queuedMessageCount);
                throw new InvalidOperationException("Failed to enqueue the message.");
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
        var shouldCompleteQueue = false;

        lock (_lifetimeLock)
        {
            if (_disposed)
            {
                return;
            }

            _completed = true;
            Volatile.Write(ref _accepting, false);
            shouldCompleteQueue = Interlocked.Read(ref _pendingMessages) == 0;
        }

        if (shouldCompleteQueue)
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
                    Interlocked.Read(ref _queuedMessageCount),
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

                while (_queue.Reader.TryRead(out var input))
                {
                    ProcessQueuedInput(input, cancellationToken);

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
                    if (!await _queue.Reader.WaitToReadAsync(waitCts.Token).ConfigureAwait(false))
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
            var retirementRequested = Volatile.Read(ref registration.RetirementRequested);
            int previousWorkerCount;
            int currentWorkerCount;

            lock (_workersLock)
            {
                previousWorkerCount = Volatile.Read(ref _workerCount);
                currentWorkerCount = Interlocked.Decrement(ref _workerCount);
            }

            if (retirementRequested && !IsTerminalDrain())
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
            MarkProcessingAttemptCompleted();
        }
    }

    private void ProcessQueuedInput(TInput input, CancellationToken cancellationToken)
    {
        Interlocked.Decrement(ref _queuedMessageCount);
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
        var queuedMessageCount = Interlocked.Read(ref _queuedMessageCount);
        var lastProbeGain = Volatile.Read(ref _lastProbeGain);

        return new DispatcherScalingStats(
            Interlocked.Read(ref _pendingMessages),
            Interlocked.Read(ref _completedMessages),
            workerCount,
            Volatile.Read(ref _desiredWorkerCount),
            busyWorkers,
            queuedMessageCount,
            Volatile.Read(ref _throughput),
            Volatile.Read(ref _smoothedThroughput),
            IsSaturated(workerCount, busyWorkers, queuedMessageCount),
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
        // await, and the worker loop starts by draining the queue. Called directly from
        // the scale controller it would execute transformers on the timer thread while holding
        // _workersLock, blocking scaling decisions and CompleteAsync/DisposeAsync.
        registration.WorkerTask = _singleWorkerMode
            ? Task.Run(() => SingleWorkerLoopAsync(_stopCts.Token))
            : Task.Run(() => WorkerLoopAsync(registration, _stopCts.Token));
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
        lock (_workersLock)
        {
            Volatile.Write(ref _desiredWorkerCount, 0);
        }

        _scaleCts.Cancel();
        _queue.Writer.TryComplete();
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
