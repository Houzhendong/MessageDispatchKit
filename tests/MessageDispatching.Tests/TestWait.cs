using System.Diagnostics;

namespace MessageDispatching.Tests;

internal static class TestWait
{
    public static async Task UntilAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        var timeoutDuration = timeout ?? TimeSpan.FromSeconds(5);
        var delay = pollInterval ?? TimeSpan.FromMilliseconds(10);

        while (!condition())
        {
            if (Stopwatch.GetElapsedTime(startTimestamp) >= timeoutDuration)
            {
                throw new TimeoutException("Timed out waiting for test condition.");
            }

            await Task.Delay(delay);
        }
    }
}
