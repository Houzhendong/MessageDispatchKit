namespace MessageDispatching;

/// <summary>
/// Receives output messages published by a dispatcher after conversion.
/// Subscriber failures are reported to <see cref="HandleError"/> and do not stop delivery to
/// other subscribers.
/// </summary>
public interface IMessageSubscriber<in TMessage>
{
    void Handle(TMessage message, CancellationToken cancellationToken);

    void HandleError(
        TMessage message,
        Exception exception,
        CancellationToken cancellationToken)
    {
    }
}
