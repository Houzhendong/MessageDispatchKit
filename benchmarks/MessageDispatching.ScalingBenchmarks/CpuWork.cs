using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.CompilerServices;
using MessageDispatching;

namespace MessageDispatching.ScalingBenchmarks;

internal sealed class CpuWorkload
{
    public CpuWorkload(string name, byte[] buffer, int passes)
    {
        Name = name;
        Buffer = buffer;
        Passes = passes;
    }

    public string Name { get; }

    public byte[] Buffer { get; }

    public int Passes { get; }
}

internal static class CpuWork
{
    public static CpuWorkload Create(string name, int byteCount, int passes, uint seed)
    {
        var buffer = new byte[byteCount];
        var state = seed == 0 ? 0x9E3779B9u : seed;

        for (var index = 0; index < buffer.Length; index++)
        {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            buffer[index] = (byte)state;
        }

        return new CpuWorkload(name, buffer, passes);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static ulong Execute(CpuWorkload workload, int salt)
    {
        const ulong prime = 1099511628211UL;
        var hash = 14695981039346656037UL ^ unchecked((uint)salt);
        var buffer = workload.Buffer;

        for (var pass = 0; pass < workload.Passes; pass++)
        {
            hash ^= unchecked((uint)(salt + pass * 0x9E37));
            for (var index = 0; index < buffer.Length; index++)
            {
                hash ^= (ulong)(buffer[index] + (byte)(index + pass));
                hash *= prime;
                hash = BitOperations.RotateLeft(hash, 5);
            }

            hash ^= (ulong)buffer.Length;
            hash *= prime;
        }

        return hash;
    }
}

internal readonly record struct KeyedCpuMessage(int Sequence, int WorkloadIndex, int Salt);

internal readonly record struct CpuInput(long Id, int WorkloadIndex, int Salt);

internal readonly record struct CpuOutput(long Id, ulong Checksum);

internal sealed class RunCounters
{
    private long _enqueued;
    private long _enqueuedIdSum;

    public long Enqueued => Interlocked.Read(ref _enqueued);

    public long EnqueuedIdSum => Interlocked.Read(ref _enqueuedIdSum);

    public void RecordEnqueued(long id)
    {
        Interlocked.Increment(ref _enqueued);
        Interlocked.Add(ref _enqueuedIdSum, id);
    }
}

internal abstract class ConcurrentWorkObserver
{
    private int _currentConcurrency;
    private int _maxConcurrency;
    private long _processed;
    private long _checksum;

    public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);

    public long Processed => Interlocked.Read(ref _processed);

    public long Checksum => Interlocked.Read(ref _checksum);

    protected void EnterWork()
    {
        var current = Interlocked.Increment(ref _currentConcurrency);
        RecordMaximum(ref _maxConcurrency, current);
    }

    protected void ExitWork(ulong checksum)
    {
        Interlocked.Add(ref _checksum, unchecked((long)checksum));
        Interlocked.Increment(ref _processed);
        Interlocked.Decrement(ref _currentConcurrency);
    }

    private static void RecordMaximum(ref int target, int value)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (value <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }
}

internal sealed class OrderedCpuHandler : ConcurrentWorkObserver, IKeyedMessageHandler<int, KeyedCpuMessage>
{
    private sealed class KeyState
    {
        public int Active;
        public int LastSequence = -1;
    }

    private readonly CpuWorkload[] _workloads;
    private readonly ConcurrentDictionary<int, KeyState> _keyStates = new();
    private long _orderingViolations;
    private long _sameKeyConcurrencyViolations;
    private long _handlerErrors;
    private string? _firstViolation;

    public OrderedCpuHandler(CpuWorkload[] workloads)
    {
        _workloads = workloads;
    }

    public long OrderingViolations => Interlocked.Read(ref _orderingViolations);

    public long SameKeyConcurrencyViolations =>
        Interlocked.Read(ref _sameKeyConcurrencyViolations);

    public long HandlerErrors => Interlocked.Read(ref _handlerErrors);

    public string? FirstViolation => Volatile.Read(ref _firstViolation);

    public void Handle(int key, KeyedCpuMessage message, CancellationToken cancellationToken)
    {
        var state = _keyStates.GetOrAdd(key, static _ => new KeyState());
        if (Interlocked.Exchange(ref state.Active, 1) != 0)
        {
            Interlocked.Increment(ref _sameKeyConcurrencyViolations);
            RecordFirstViolation($"concurrent-key-{key}");
        }

        EnterWork();
        var checksum = 0UL;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var expectedSequence = Volatile.Read(ref state.LastSequence) + 1;
            if (message.Sequence != expectedSequence)
            {
                Interlocked.Increment(ref _orderingViolations);
                RecordFirstViolation(
                    $"key-{key}-expected-{expectedSequence}-actual-{message.Sequence}");
            }

            checksum = CpuWork.Execute(_workloads[message.WorkloadIndex], message.Salt);
            Volatile.Write(ref state.LastSequence, message.Sequence);
        }
        finally
        {
            Volatile.Write(ref state.Active, 0);
            ExitWork(checksum);
        }
    }

    public void HandleError(
        int key,
        KeyedCpuMessage message,
        Exception exception,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _handlerErrors);
        RecordFirstViolation($"handler-error-{exception.GetType().Name}");
    }

    private void RecordFirstViolation(string message)
    {
        Interlocked.CompareExchange(ref _firstViolation, message, null);
    }
}

internal sealed class CpuTransformer : ConcurrentWorkObserver, IMessageTransformer<CpuInput, CpuOutput>
{
    private readonly CpuWorkload[] _workloads;
    private long _transformErrors;
    private string? _firstError;

    public CpuTransformer(CpuWorkload[] workloads)
    {
        _workloads = workloads;
    }

    public long TransformErrors => Interlocked.Read(ref _transformErrors);

    public string? FirstError => Volatile.Read(ref _firstError);

    public CpuOutput Transform(CpuInput input, CancellationToken cancellationToken)
    {
        EnterWork();
        var checksum = 0UL;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            checksum = CpuWork.Execute(_workloads[input.WorkloadIndex], input.Salt);
            return new CpuOutput(input.Id, checksum);
        }
        finally
        {
            ExitWork(checksum);
        }
    }

    public void HandleError(
        CpuInput input,
        Exception exception,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _transformErrors);
        Interlocked.CompareExchange(
            ref _firstError,
            $"transform-error-{exception.GetType().Name}",
            null);
    }
}

internal sealed class CpuSubscriber : IMessageSubscriber<CpuOutput>
{
    private long _published;
    private long _idSum;
    private long _checksum;
    private long _subscriberErrors;

    public long Published => Interlocked.Read(ref _published);

    public long IdSum => Interlocked.Read(ref _idSum);

    public long Checksum => Interlocked.Read(ref _checksum);

    public long SubscriberErrors => Interlocked.Read(ref _subscriberErrors);

    public void Handle(CpuOutput message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Interlocked.Increment(ref _published);
        Interlocked.Add(ref _idSum, message.Id);
        Interlocked.Add(ref _checksum, unchecked((long)message.Checksum));
    }

    public void HandleError(
        CpuOutput message,
        Exception exception,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _subscriberErrors);
    }
}
