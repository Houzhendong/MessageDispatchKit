namespace MessageDispatching;

/// <summary>
/// Handles a deserialized message for a given key. Implementations run synchronously on a
/// dispatcher worker thread. Messages for the same key are handed to <see cref="Handle"/> in
/// strict FIFO order, and never concurrently for the same key.
/// Handler failures are reported to <see cref="HandleError"/> on the same worker thread.
/// </summary>
public interface IKeyedMessageHandler<in TKey, in TMessage>
{
    void Handle(TKey key, TMessage message, CancellationToken cancellationToken);

    void HandleError(
        TKey key,
        TMessage message,
        Exception exception,
        CancellationToken cancellationToken)
    {
    }
}
