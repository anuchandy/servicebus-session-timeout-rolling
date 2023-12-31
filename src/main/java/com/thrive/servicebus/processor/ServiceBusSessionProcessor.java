package com.thrive.servicebus.processor;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.thrive.servicebus.processor.implementation.RetryableConcurrentSessionsPumping;
import reactor.core.Disposable;
import reactor.core.Disposables;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * A type that pumps messages across concurrent sessions. The pumping starts once the {@link ServiceBusSessionProcessor#start()}
 * is called. Disposing the {@link Disposable} returned by the start API will tear down the pumping.
 * <p>
 * The start API can be called only once. Tearing down is not an operation performed frequently, once teared down,
 * to restart create a new {@link ServiceBusSessionProcessor} and call start on it.
 * </p>
 */
public final class ServiceBusSessionProcessor {
    /**
     * Load balance the concurrency across the pumps, at most 30 concurrent sessions per pump.
     */
    private static final int SESSIONS_PER_PUMP = 30;
    private final int[] maxConcurrentSessionsPerPump;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final String connectionString;
    private final String queueName;
    private final String topicName;
    private final String subscriptionName;
    private final Duration maxSessionLockRenewal;
    private final Duration sessionTimeout;
    private BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage;
    private final String namespace;
    private final Disposable.Composite disposable = Disposables.composite();

    public ServiceBusSessionProcessor(String connectionString, String topicName, String subscriptionName,
        int maxConcurrentSessions, Duration maxSessionLockRenewal, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, String namespace) {
        this.connectionString = Objects.requireNonNull(connectionString);
        this.queueName = null;
        this.topicName = Objects.requireNonNull(topicName);
        this.subscriptionName = Objects.requireNonNull(subscriptionName);
        this.maxSessionLockRenewal = Objects.requireNonNull(maxSessionLockRenewal);
        this.sessionTimeout = Objects.requireNonNull(sessionTimeout);
        this.onMessage = Objects.requireNonNull(onMessage);
        this.namespace = Objects.requireNonNull(namespace);
        this.maxConcurrentSessionsPerPump = splitPerPump(maxConcurrentSessions);
    }

    public ServiceBusSessionProcessor(String connectionString, String queueName,
        int maxConcurrentSessions, Duration maxSessionLockRenewal, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, String namespace) {
        this.connectionString = Objects.requireNonNull(connectionString);
        this.queueName = Objects.requireNonNull(queueName);
        this.topicName = null;
        this.subscriptionName = null;
        this.maxSessionLockRenewal = Objects.requireNonNull(maxSessionLockRenewal);
        this.sessionTimeout = Objects.requireNonNull(sessionTimeout);
        this.onMessage = Objects.requireNonNull(onMessage);
        this.namespace = Objects.requireNonNull(namespace);
        this.maxConcurrentSessionsPerPump = splitPerPump(maxConcurrentSessions);
    }

    public Disposable start() {
        if (isStarted.getAndSet(true)) {
            throw new IllegalStateException("The start() cannot be called more then once.");
        }
        if (disposable.isDisposed()) {
            throw new IllegalStateException("The start() cannot be called after disposal.");
        }
        final int c = maxConcurrentSessionsPerPump.length;
        final boolean isQueue = queueName != null;
        for (int i = 0; i < c; i++) {
            final RetryableConcurrentSessionsPumping pumping;
            if (isQueue) {
                pumping = new RetryableConcurrentSessionsPumping(i, connectionString, queueName,
                        maxConcurrentSessionsPerPump[i], maxSessionLockRenewal, sessionTimeout, onMessage, namespace);
            } else {
                pumping = new RetryableConcurrentSessionsPumping(i, connectionString, topicName, subscriptionName,
                        maxConcurrentSessionsPerPump[i], maxSessionLockRenewal, sessionTimeout, onMessage, namespace);
            }
            if (!disposable.add(pumping.begin())) {
                throw new IllegalStateException("The start() cannot be called after disposal.");
            }
        }
        return disposable;
    }

    private static int [] splitPerPump(int maxConcurrentSessions) {
        final int [] maxConcurrentSessionsPerPump;
        if (maxConcurrentSessions <= SESSIONS_PER_PUMP) {
            maxConcurrentSessionsPerPump = new int[1];
            maxConcurrentSessionsPerPump[0] = maxConcurrentSessions;
        } else {
            final int a = maxConcurrentSessions / SESSIONS_PER_PUMP;
            final int d = maxConcurrentSessions % SESSIONS_PER_PUMP;
            if (d <= SESSIONS_PER_PUMP / 4) {
                maxConcurrentSessionsPerPump = new int[a];
                Arrays.fill(maxConcurrentSessionsPerPump, SESSIONS_PER_PUMP);
                for (int i = 0; i < d; i++) {
                    maxConcurrentSessionsPerPump[i % a] += 1;
                }
            } else {
                maxConcurrentSessionsPerPump = new int[a + 1];
                Arrays.fill(maxConcurrentSessionsPerPump, 0, a, SESSIONS_PER_PUMP);
                maxConcurrentSessionsPerPump[a] = d;
            }
        }
        return maxConcurrentSessionsPerPump;
    }
}