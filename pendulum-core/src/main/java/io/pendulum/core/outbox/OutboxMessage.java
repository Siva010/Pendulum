package io.pendulum.core.outbox;

import java.util.Map;
import java.util.UUID;

/**
 * A durable record of an intent to cause an effect outside this database.
 *
 * @param destination where it is going — a Kafka topic, a webhook name, a provider id
 * @param messageKey  a stable key the consumer can deduplicate on. Delivery is at-least-once and
 *                    cannot be otherwise, so rather than pretend, the engine hands every consumer
 *                    the means to make a replay harmless.
 */
public record OutboxMessage(
        UUID id,
        String tenantId,
        String destination,
        String payload,
        Map<String, String> headers,
        String messageKey,
        int attempts,
        int maxAttempts
) {

    public OutboxMessage {
        if (id == null) throw new IllegalArgumentException("id is required");
        if (tenantId == null || tenantId.isBlank()) throw new IllegalArgumentException("tenantId is required");
        if (destination == null || destination.isBlank()) throw new IllegalArgumentException("destination is required");
        if (payload == null) payload = "{}";
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        if (maxAttempts <= 0) maxAttempts = 10;
    }

    public static Builder to(String tenantId, String destination) {
        return new Builder(tenantId, destination);
    }

    public boolean isFinalAttempt() {
        return attempts >= maxAttempts;
    }

    public static final class Builder {
        private final String tenantId;
        private final String destination;
        private UUID id = UUID.randomUUID();
        private String payload = "{}";
        private Map<String, String> headers = Map.of();
        private String messageKey;
        private int maxAttempts = 10;

        private Builder(String tenantId, String destination) {
            this.tenantId = tenantId;
            this.destination = destination;
        }

        public Builder id(UUID id) { this.id = id; return this; }
        public Builder payload(String json) { this.payload = json; return this; }
        public Builder headers(Map<String, String> headers) { this.headers = headers; return this; }
        public Builder messageKey(String messageKey) { this.messageKey = messageKey; return this; }
        public Builder maxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; return this; }

        public OutboxMessage build() {
            return new OutboxMessage(id, tenantId, destination, payload, headers, messageKey, 0, maxAttempts);
        }
    }
}
