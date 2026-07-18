using MessageDispatching;
using Xunit;

namespace MessageDispatching.Tests;

public sealed class HashPartitionerTests
{
    [Fact]
    public void SameKeyAlwaysMapsToSamePartition()
    {
        var first = HashPartitioner.GetPartition("order-42", 1000);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal(first, HashPartitioner.GetPartition("order-42", 1000));
        }
    }

    [Fact]
    public void SameCompositeKeyAlwaysMapsToSamePartition()
    {
        var first = HashPartitioner.GetPartition("tenant-7", 42L, 1000);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal(first, HashPartitioner.GetPartition("tenant-7", 42L, 1000));
        }
    }

    [Fact]
    public void PartitionsStayWithinRange()
    {
        const int partitionCount = 1000;

        for (var key = 0; key < 100_000; key++)
        {
            var partition = HashPartitioner.GetPartition(key, partitionCount);
            Assert.InRange(partition, 0, partitionCount - 1);
        }
    }

    [Fact]
    public void SequentialKeysSpreadAcrossPartitions()
    {
        const int partitionCount = 256;

        var used = new HashSet<int>();
        for (var key = 0; key < 100_000; key++)
        {
            used.Add(HashPartitioner.GetPartition(key, partitionCount));
        }

        // fmix should scatter sequential ids across essentially every partition.
        Assert.Equal(partitionCount, used.Count);
    }

    [Fact]
    public void CompositeKeyOrderIsSignificant()
    {
        // (a, b) and (b, a) should generally land in different partitions; the parts are combined
        // in argument order, not as an unordered set.
        var forward = HashPartitioner.GetPartition("a", "b", 1000);
        var reversed = HashPartitioner.GetPartition("b", "a", 1000);

        Assert.NotEqual(forward, reversed);
    }

    [Fact]
    public void AllArityOverloadsProducePartitionsInRange()
    {
        const int partitionCount = 500;

        Assert.InRange(HashPartitioner.GetPartition(1, partitionCount), 0, partitionCount - 1);
        Assert.InRange(HashPartitioner.GetPartition(1, 2, partitionCount), 0, partitionCount - 1);
        Assert.InRange(HashPartitioner.GetPartition(1, 2, 3, partitionCount), 0, partitionCount - 1);
        Assert.InRange(HashPartitioner.GetPartition(1, 2, 3, 4, partitionCount), 0, partitionCount - 1);
        Assert.InRange(HashPartitioner.GetPartition(1, 2, 3, 4, 5, partitionCount), 0, partitionCount - 1);
    }

    [Fact]
    public void NonPositivePartitionCountThrows()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => HashPartitioner.GetPartition("k", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => HashPartitioner.GetPartition("k", -1));
    }
}
