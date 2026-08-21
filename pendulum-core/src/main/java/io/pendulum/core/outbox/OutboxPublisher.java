package io.pendulum.core.outbox;

/**
 * Performs the effect the outbox recorded — produce to Kafka, POST a webhook, call a provider.
 *
 * <p>Kept an interface with no implementation in core on purpose: the moment {@code pendulum-core}
 * has a Kafka client on its classpath, embedding the engine drags a broker dependency along with
 * it. Adapters belong at the edges.
 *
 * <p>Implementations should treat {@link OutboxMessage#messageKey()} as the consumer's
 * deduplication key and set it on the outgoing message wherever the transport allows, because
 * delivery here is at-least-once.
 */
@FunctionalInterface
public interface OutboxPublisher {

    /**
     * @throws Exception to signal delivery failed. The relay retries with backoff until the
     *                   message's attempt budget is exhausted, then dead-letters it.
     */
    void publish(OutboxMessage message) throws Exception;
}
