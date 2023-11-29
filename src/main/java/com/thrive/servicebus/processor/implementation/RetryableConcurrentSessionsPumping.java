package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.thrive.servicebus.processor.DispositionOperations;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.azure.core.util.FluxUtil.monoError;

/**
 * A type listens to the error from the current {@link ConcurrentSessionsPumping#begin()} API and restart the pumping by
 * creating a new {@link ConcurrentSessionsPumping}.
 */
public final class RetryableConcurrentSessionsPumping {
    private static final RuntimeException DISPOSED_ERROR = new RuntimeException("RetryableConcurrentSessionsPumping is disposed.");
    /**
     * If the current {@link ConcurrentSessionsPumping} terminates, then some serious event happened (service upgrade,
     * service throttling, network downtime), use 5 second backoff before retrying the connection to restart the pumping.
     */
    private static final Duration NEXT_PUMP_BACKOFF = Duration.ofSeconds(5);
    private final ClientLogger logger;
    private final int idPrefix;
    private final AtomicLong idSuffix = new AtomicLong(0);
    private final AtomicBoolean hasBegan = new AtomicBoolean(false);
    private final String connectionString;
    private final String queueName;
    private final String topicName;
    private final String subscriptionName;
    private final int maxConcurrentSessions;
    private final Duration maxSessionLockRenewal;
    private final Duration sessionTimeout;
    private BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage;
    private final String entityPath;
    private final String namespace;
    private final Disposable.Composite disposable = Disposables.composite();

    public RetryableConcurrentSessionsPumping(int idPrefix, String connectionString, String queueName,
        int maxConcurrentSessions, Duration maxSessionLockRenewal, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, String namespace) {
        this.logger = new ClientLogger(RetryableConcurrentSessionsPumping.class);
        this.idPrefix = idPrefix;
        this.connectionString = Objects.requireNonNull(connectionString);
        this.queueName = Objects.requireNonNull(queueName);
        this.topicName = null;
        this.subscriptionName = null;
        this.maxConcurrentSessions = maxConcurrentSessions;
        this.maxSessionLockRenewal = Objects.requireNonNull(maxSessionLockRenewal);
        this.sessionTimeout = Objects.requireNonNull(sessionTimeout);
        this.onMessage = Objects.requireNonNull(onMessage);
        this.namespace = Objects.requireNonNull(namespace);
        this.entityPath = queueName;
    }

    public RetryableConcurrentSessionsPumping(int idPrefix, String connectionString, String topicName, String subscriptionName,
        int maxConcurrentSessions, Duration maxSessionLockRenewal, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, String namespace) {
        this.logger = new ClientLogger(RetryableConcurrentSessionsPumping.class);
        this.idPrefix = idPrefix;
        this.connectionString = Objects.requireNonNull(connectionString);
        this.queueName = null;
        this.topicName = Objects.requireNonNull(topicName);
        this.subscriptionName = Objects.requireNonNull(subscriptionName);
        this.maxConcurrentSessions = maxConcurrentSessions;
        this.maxSessionLockRenewal = Objects.requireNonNull(maxSessionLockRenewal);
        this.sessionTimeout = Objects.requireNonNull(sessionTimeout);
        this.onMessage = Objects.requireNonNull(onMessage);
        this.namespace = Objects.requireNonNull(namespace);
        this.entityPath = String.format(Locale.ROOT, "%s/subscriptions/%s", topicName, subscriptionName);
    }

    public Disposable begin() {
        if (hasBegan.getAndSet(true)) {
            throw logger.atInfo().log(new IllegalStateException("The pumping cannot begin more than once."));
        }
        final Mono<Void> pumping = Mono.usingWhen(
                buildSessionProvider(),
                provider -> {
                    String pumpId = String.format(Locale.ROOT, "%d-%d", idPrefix, idSuffix.getAndIncrement());
                    final ConcurrentSessionsPumping pump = new ConcurrentSessionsPumping(pumpId, maxConcurrentSessions, sessionTimeout,
                            onMessage, namespace, entityPath, provider);
                    return pump.begin();
                },
                provider -> provider.closeAsync());
        final Mono<Void> retryablePump = pumping.retryWhen(retrySpecForNextPump());
        final Disposable d = retryablePump.subscribe();
        if (!disposable.add(d)) {
            throw logger.atInfo().log(new IllegalStateException("Cannot begin pumping after the disposal."));
        }
        return disposable;
    }

    private Mono<SessionProvider> buildSessionProvider() {
        return Mono.fromSupplier(() -> {
            String pumpId = String.format(Locale.ROOT, "%d-%d", idPrefix, idSuffix.getAndIncrement());
            if (queueName != null) {
                return new SessionProvider(pumpId, connectionString, queueName, maxSessionLockRenewal, namespace, entityPath);
            } else {
                return new SessionProvider(pumpId, connectionString, topicName, subscriptionName, maxSessionLockRenewal, namespace, entityPath);
            }
        });
    }

    private Retry retrySpecForNextPump() {
        return Retry.from(retrySignals -> retrySignals
                .concatMap(retrySignal -> {
                    final Retry.RetrySignal signal = retrySignal.copy();
                    final Throwable error = signal.failure();
                    if (error == null) {
                        return monoError(logger,
                                new IllegalStateException("RetrySignal::failure() not expected to be null."));
                    }
                    if (!(error instanceof SessionsPumpTerminatedException)) {
                        return monoError(logger,
                                new IllegalStateException("RetrySignal::failure() expected to be SessionsPumpTerminatedException.", error));
                    }

                    final SessionsPumpTerminatedException e = (SessionsPumpTerminatedException) error;

                    if (disposable.isDisposed()) {
                        e.log(logger,
                                "The streaming is disposed, canceling retry for the next ConcurrentSessionsPumping.", true);
                        return Mono.error(DISPOSED_ERROR);
                    }

                    e.log(logger,
                            "The current ConcurrentSessionsPumping is terminated, scheduling retry for the next pump.", true);

                    return Mono.delay(NEXT_PUMP_BACKOFF, Schedulers.boundedElastic())
                            .handle((v, sink) -> {
                                if (disposable.isDisposed()) {
                                    e.log(logger,
                                            "During backoff, the streaming is disposed, canceling retry for the next ConcurrentSessionsPumping.", false);
                                    sink.error(DISPOSED_ERROR);
                                } else {
                                    e.log(logger,
                                            "Retrying for the next ConcurrentSessionsPumping.", false);
                                    sink.next(v);
                                }
                            });
                }));
    }
}