package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.thrive.servicebus.processor.DispositionOperations;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.azure.core.util.FluxUtil.monoError;

/**
 * A type that tries to keep a live {@link SessionReceiver} around. If this type receives {@link SessionEndedException}
 * from the {@link SessionReceiver} it currently manages, it will attempt to obtain (a.k.a roll to) a new
 * {@link SessionReceiver} using {@link SessionProvider}.
 * <p>
 * The type will keep pumping messages from the current {@link SessionReceiver}, transparently roll and continue
 * pumping from the next {@link SessionReceiver} when the last one terminates. The pumping starts once
 * {@link RollingSessionReceiver#begin(Scheduler)} is called and caller subscribes to the Mono this API returned.
 * </p>
 * <p>
 * If the type is unable to roll to a new {@link SessionReceiver} due to an error from the {@link SessionProvider},
 * it will stop pumping and terminate Mono that the begin API returned by signaling the provider error.
 * </p>
 */
final class RollingSessionReceiver {

    // A coarse backoff before rolling to the next session.
    private static final Duration ACCEPT_SESSION_BACKOFF = Duration.ofSeconds(3);
    private final int rollerId;
    private final ClientLogger logger;
    private final SessionProvider sessionProvider;
    private final Duration sessionTimeout;
    private final BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage;

    RollingSessionReceiver(int rollerId, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, SessionProvider sessionProvider) {
        this.rollerId = rollerId;
        final Map<String, Object> loggingContext = new HashMap<>(4);
        loggingContext.put("pumpId", sessionProvider.getPumpId());
        loggingContext.put("rollerId", rollerId);
        loggingContext.put("namespace", sessionProvider.getNamespace());
        loggingContext.put("entityPath", sessionProvider.getEntityPath());
        this.logger = new ClientLogger(RollingSessionReceiver.class, loggingContext);
        this.sessionTimeout = sessionTimeout;
        this.onMessage = onMessage;
        this.sessionProvider = sessionProvider;
    }

    Mono<Void> begin(Scheduler pumpScheduler) {
        return sessionProvider.mono()
                .flatMap(sessionReceiver -> sessionReceiver.begin(rollerId, sessionTimeout, onMessage, pumpScheduler))
                .retryWhen(retrySpecForAcceptSession());
    }

    private Retry retrySpecForAcceptSession() {
        return Retry.from(retrySignals -> retrySignals
                .concatMap(retrySignal -> {
                    final Retry.RetrySignal signal = retrySignal.copy();
                    final Throwable error = signal.failure();
                    if (error == null) {
                        return monoError(logger, new IllegalStateException("RetrySignal::failure() not expected to be null."));
                    }
                    if ((error instanceof SessionEndedException)) {
                        final SessionEndedException e = (SessionEndedException) error;
                        e.log(logger,
                                "The current session is terminated, scheduling accept next SessionReceiver.", true);
                        return Mono.delay(ACCEPT_SESSION_BACKOFF, Schedulers.boundedElastic())
                                .handle((v, sink) -> {
                                    e.log(logger, "Attempting next SessionReceiver accept.", false);
                                    sink.next(v);
                                });
                    }
                    // Propagate 'SessionProviderException' to downstream.
                    return Mono.error(error);
                }));
    }
}