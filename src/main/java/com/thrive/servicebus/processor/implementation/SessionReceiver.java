package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import com.azure.core.util.logging.LoggingEventBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.thrive.servicebus.processor.DispositionOperations;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * A receiver that is attached to a Service Bus session. The receiver will start pumping messages from the session
 * once the {@link SessionReceiver#begin(int, Duration, BiConsumer, Scheduler)} API is called and the caller subscribes
 * to the Mono this API returned.
 * <p>
 *  The receiver terminates when it gets disconnected from the session. The disconnect can happen if Service Bus forcefully
 *  detaches the session (e.g., server error, session lock renewal expired), if there is a network outage,
 *  or if there is no message on the session within the sessionTimeout provided to the begin API. The Mono returned by
 *  the begin API terminates with {@link SessionEndedException} once the SessionReceiver gets disconnected from the session.
 * </p>
 */
final class SessionReceiver {
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);
    private final ClientLogger logger;
    private final String sessionId;
    private final ServiceBusReceiverAsyncClient client;
    private final DispositionOperations dispositionOperations;
    SessionReceiver(String pumpId, String namespace, String entityPath, ServiceBusReceiverAsyncClient client) {
        this.sessionId = client.getSessionId();
        final Map<String, Object> loggingContext = new HashMap<>(4);
        loggingContext.put("namespace", namespace);
        loggingContext.put("entityPath", entityPath);
        loggingContext.put("pumpId", pumpId);
        loggingContext.put("sessionId", sessionId);
        this.logger = new ClientLogger(SessionReceiver.class, loggingContext);
        this.client = client;
        this.dispositionOperations = new DispositionOperationsImpl(logger, client);
    }
    Mono<Void> begin(int rollerId, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage, Scheduler pumpScheduler, Scheduler timeoutScheduler) {
        return Mono.usingWhen(Mono.just(0),
                __ ->  {
                    final SessionIdleTimer sessionIdleTimer = new SessionIdleTimer(logger, sessionId, timeoutScheduler, sessionTimeout);
                    return client.receiveMessages()
                            .takeUntilOther(sessionIdleTimer.timeout())
                            .flatMap(message -> Mono.fromRunnable(() -> {
                                sessionIdleTimer.cancel();
                                try {
                                    onMessage.accept(message, dispositionOperations);
                                } catch (Exception e) {
                                    withRollerId(logger.atVerbose(), rollerId).log("Ignoring error from onMessage handler.", e);
                                }
                                sessionIdleTimer.start();
                            })
                            .subscribeOn(pumpScheduler), 1, 1)
                    .onErrorMap(e -> SessionEndedException.forError(sessionId, e))
                    .then(Mono.defer(() -> Mono.error(SessionEndedException.forCompletion(sessionId))));
                },
                (__) -> terminate(rollerId, TerminalSignalType.COMPLETED),
                (__, e) -> terminate(rollerId, TerminalSignalType.ERRORED),
                (__) -> terminate(rollerId, TerminalSignalType.CANCELED)
        );
    }

    private Mono<Void> terminate(int rollerId, TerminalSignalType signalType) {
        if (isTerminated.getAndSet(true)) {
            return Mono.empty();
        }
        withRollerId(logger.atInfo(), rollerId).log("Session terminated, signal: {}", signalType);
        final Mono<Void> closeMono = Mono.fromRunnable(() -> {
            client.close();
        });
        if (Schedulers.isInNonBlockingThread()) {
            return closeMono.subscribeOn(Schedulers.boundedElastic());
        } else {
            return closeMono;
        }
    }
    private static LoggingEventBuilder withRollerId(LoggingEventBuilder builder, int rollerId) {
        return builder.addKeyValue("rollerId", rollerId);
    }
    private static class DispositionOperationsImpl implements DispositionOperations {
        private final ClientLogger logger;
        private final ServiceBusReceiverAsyncClient client;

        DispositionOperationsImpl(ClientLogger logger, ServiceBusReceiverAsyncClient client) {
            this.logger = logger;
            this.client = client;
        }
        @Override
        public void complete(ServiceBusReceivedMessage message) {
            try {
                client.complete(message).block();
            } catch (Exception e) {
                logger.atVerbose().log("Failed to complete message", e);
            }
        }
        @Override
        public void abandon(ServiceBusReceivedMessage message) {
            try {
                client.abandon(message).block();
            } catch (Exception e) {
                logger.atVerbose().log("Failed to abandon message", e);
            }
        }
    }
}