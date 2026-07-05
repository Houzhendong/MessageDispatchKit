namespace MessageDispatching;

/// <summary>
/// Converts an input message into the published output message type. Implementations run
/// synchronously on dispatcher worker threads.
/// Transform failures are reported to <see cref="HandleError"/> on a dispatcher worker thread.
/// </summary>
public interface IMessageTransformer<in TInput, out TOutput>
{
    TOutput Transform(TInput input, CancellationToken cancellationToken);

    void HandleError(
        TInput input,
        Exception exception,
        CancellationToken cancellationToken)
    {
    }
}
