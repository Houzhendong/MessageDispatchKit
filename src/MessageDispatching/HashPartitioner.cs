namespace MessageDispatching;

/// <summary>
/// Maps a large or unbounded logical key space onto a fixed number of partitions using a stable
/// hash, so a keyed dispatcher can be driven with a bounded partition count (keeping the internal
/// key map small and its memory bounded). Overloads accept composite keys of up to five parts.
///
/// <para>
/// The mapping is deterministic within a process: the same logical key (or the same tuple of key
/// parts, in order) always yields the same partition. When the returned partition id is used as
/// the dispatcher key, this preserves per-logical-key FIFO ordering, because all messages for one
/// logical key land in the same partition queue and are handled in arrival order.
/// </para>
/// <para>
/// The trade-off: distinct logical keys that collide onto the same partition are serialized
/// relative to each other. They lose the cross-key parallelism they would have had with a
/// dedicated key, and a slow logical key head-of-line blocks the others sharing its partition.
/// Choose <c>partitionCount</c> large enough to keep collisions (and thus lost parallelism)
/// acceptable for the active logical-key count, while staying within the range that keeps the
/// dispatcher's key map cheap.
/// </para>
/// <para>
/// Note: hash codes are randomized per process for some types (e.g. <see cref="string"/>), so
/// partition assignments are stable only within a single process lifetime. Do not persist
/// partition ids or rely on them across restarts.
/// </para>
/// </summary>
public static class HashPartitioner
{
    // FNV-1a 32-bit constants: the offset basis seeds the accumulator, the prime mixes each part.
    private const uint FnvOffsetBasis = 2166136261;
    private const uint FnvPrime = 16777619;

    /// <summary>
    /// Returns the partition id in the range [0, <paramref name="partitionCount"/>) for the key.
    /// The same key always maps to the same partition within a process.
    /// </summary>
    public static int GetPartition<T1>(T1 key1, int partitionCount)
    {
        ValidatePartitionCount(partitionCount);

        var h = Combine(FnvOffsetBasis, key1);
        return Reduce(h, partitionCount);
    }

    /// <summary>
    /// Returns the partition id for the ordered pair of key parts. Parts are combined in argument
    /// order, so <c>(a, b)</c> and <c>(b, a)</c> generally map to different partitions.
    /// </summary>
    public static int GetPartition<T1, T2>(T1 key1, T2 key2, int partitionCount)
    {
        ValidatePartitionCount(partitionCount);

        var h = Combine(FnvOffsetBasis, key1);
        h = Combine(h, key2);
        return Reduce(h, partitionCount);
    }

    /// <summary>Returns the partition id for the ordered triple of key parts.</summary>
    public static int GetPartition<T1, T2, T3>(T1 key1, T2 key2, T3 key3, int partitionCount)
    {
        ValidatePartitionCount(partitionCount);

        var h = Combine(FnvOffsetBasis, key1);
        h = Combine(h, key2);
        h = Combine(h, key3);
        return Reduce(h, partitionCount);
    }

    /// <summary>Returns the partition id for the ordered quadruple of key parts.</summary>
    public static int GetPartition<T1, T2, T3, T4>(
        T1 key1,
        T2 key2,
        T3 key3,
        T4 key4,
        int partitionCount)
    {
        ValidatePartitionCount(partitionCount);

        var h = Combine(FnvOffsetBasis, key1);
        h = Combine(h, key2);
        h = Combine(h, key3);
        h = Combine(h, key4);
        return Reduce(h, partitionCount);
    }

    /// <summary>Returns the partition id for the ordered quintuple of key parts.</summary>
    public static int GetPartition<T1, T2, T3, T4, T5>(
        T1 key1,
        T2 key2,
        T3 key3,
        T4 key4,
        T5 key5,
        int partitionCount)
    {
        ValidatePartitionCount(partitionCount);

        var h = Combine(FnvOffsetBasis, key1);
        h = Combine(h, key2);
        h = Combine(h, key3);
        h = Combine(h, key4);
        h = Combine(h, key5);
        return Reduce(h, partitionCount);
    }

    private static uint Combine<T>(uint accumulator, T value)
    {
        var hashCode = (uint)EqualityComparer<T>.Default.GetHashCode(value!);
        return (accumulator ^ hashCode) * FnvPrime;
    }

    private static int Reduce(uint hash, int partitionCount)
    {
        // fmix32 (the MurmurHash3 finalizer) avalanches the accumulated hash so that low-entropy
        // or sequential keys (auto-increment ids, adjacent strings) don't cluster after the modulo.
        hash ^= hash >> 16;
        hash *= 0x7feb352d;
        hash ^= hash >> 15;
        hash *= 0x846ca68b;
        hash ^= hash >> 16;

        return (int)(hash % (uint)partitionCount);
    }

    private static void ValidatePartitionCount(int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitionCount),
                "partitionCount must be greater than zero.");
        }
    }
}
