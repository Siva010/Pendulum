package io.pendulum.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Externalised worker tuning. These are the knobs an operator actually turns, and every one of
 * them has a failure mode attached — which is why they are configuration and not constants.
 */
@ConfigurationProperties(prefix = "pendulum")
public record PendulumProperties(
        String queue,
        int workers,
        int batchSize,
        int maxConcurrency,
        Duration leaseDuration,
        Duration heartbeatInterval,
        Duration minPollInterval,
        Duration maxPollInterval,
        Duration drainTimeout,
        Duration reapInterval,
        int reapBatchSize
) {

    public PendulumProperties {
        if (queue == null) queue = "default";
        if (workers <= 0) workers = 1;
        if (batchSize <= 0) batchSize = 16;
        if (maxConcurrency <= 0) maxConcurrency = 64;
        if (leaseDuration == null) leaseDuration = Duration.ofSeconds(30);
        if (heartbeatInterval == null) heartbeatInterval = Duration.ofSeconds(5);
        if (minPollInterval == null) minPollInterval = Duration.ofMillis(50);
        if (maxPollInterval == null) maxPollInterval = Duration.ofSeconds(2);
        if (drainTimeout == null) drainTimeout = Duration.ofSeconds(20);
        if (reapInterval == null) reapInterval = Duration.ofSeconds(1);
        if (reapBatchSize <= 0) reapBatchSize = 200;
    }
}
