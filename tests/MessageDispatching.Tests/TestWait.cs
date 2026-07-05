namespace MessageDispatching.Tests;

internal static class TestWait
{
    public static async Task UntilAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        var delay = pollInterval ?? TimeSpan.FromMilliseconds(10);

        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for test condition.");
            }

            await Task.Delay(delay);
        }
    }
}
