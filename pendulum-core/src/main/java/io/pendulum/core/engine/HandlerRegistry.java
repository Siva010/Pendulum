package io.pendulum.core.engine;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps {@code job_type} to the code that runs it. */
public final class HandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public HandlerRegistry register(String jobType, JobHandler handler) {
        JobHandler previous = handlers.putIfAbsent(jobType, handler);
        if (previous != null) {
            throw new IllegalStateException("a handler is already registered for job type " + jobType);
        }
        return this;
    }

    public Optional<JobHandler> lookup(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }

    public java.util.Set<String> registeredTypes() {
        return Map.copyOf(handlers).keySet();
    }
}
