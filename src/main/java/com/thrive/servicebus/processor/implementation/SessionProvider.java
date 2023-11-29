package com.thrive.servicebus.processor.implementation;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverAsyncClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The type which exposes a Mono {@link SessionProvider#mono()}, that when subscribed each time, produces a new
 * {@link SessionReceiver}. The produced {@link SessionReceiver} will be attached to a session obtained from
 * the Service Bus.
 * <p>
 * If an attempt to obtain a new session from the Service Bus errors, then {@link SessionProvider} mark itself as
 * terminated and any future subscriptions to the Mono will receives the error signal as {@link SessionProviderException}.
 * </p>
 */
final class SessionProvider {
    /**
     * The key to opt in the new V2 stack in version "com.azure:azure-messaging-servicebus:7.15.0-beta.5".
     * @see <a href="https://github.com/Azure/azure-sdk-for-java/issues/33688#issuecomment-1796420919">V2 Stack Flags</a>
     */
    private static final String REACTIVE_SESSION_RECEIVE_V2_STACK_KEY = "com.azure.messaging.servicebus.session.reactor.asyncReceive.v2";
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);
    private final String pumpId;
    private final String namespace;
    private final String entityPath;
    private final ServiceBusSessionReceiverAsyncClient sessionAcceptor;

    SessionProvider(String pumpId, String connectionString, String queueName,
        Duration maxSessionLockRenewal, String namespace, String entityPath) {
        this.pumpId = pumpId;
        this.namespace = namespace;
        this.entityPath = entityPath;
        this.sessionAcceptor = buildSessionAcceptor(connectionString, queueName, null, null, maxSessionLockRenewal);
    }

    SessionProvider(String pumpId, String connectionString, String topicName, String subscriptionName,
        Duration maxSessionLockRenewal, String namespace, String entityPath) {
        this.pumpId = pumpId;
        this.namespace = namespace;
        this.entityPath = entityPath;
        this.sessionAcceptor = buildSessionAcceptor(connectionString, null, topicName, subscriptionName, maxSessionLockRenewal);
    }

    String getPumpId() {
        return pumpId;
    }

    String getNamespace() {
        return namespace;
    }

    String getEntityPath() {
        return entityPath;
    }

    Mono<SessionReceiver> mono() {
        return Mono.defer(this::sessionSupplier);
    }

    Mono<Void> closeAsync() {
        isTerminated.set(true);
        final Mono<Void> closeMono = Mono.fromRunnable(() -> {
            sessionAcceptor.close();
        });
        if (Schedulers.isInNonBlockingThread()) {
            return closeMono.subscribeOn(Schedulers.boundedElastic());
        } else {
            return closeMono;
        }
    }

    private ServiceBusSessionReceiverAsyncClient buildSessionAcceptor(String connectionString, String queueName,
        String topicName, String subscriptionName, Duration maxSessionLockRenewal) {
        if (queueName != null) {
            return new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .configuration(new com.azure.core.util.ConfigurationBuilder()
                            .putProperty(REACTIVE_SESSION_RECEIVE_V2_STACK_KEY, "true")
                            .build())
                    .sessionReceiver()
                    .queueName(queueName)
                    .disableAutoComplete()
                    .maxAutoLockRenewDuration(maxSessionLockRenewal)
                    .buildAsyncClient();
        } else {
            return new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .configuration(new com.azure.core.util.ConfigurationBuilder()
                            .putProperty(REACTIVE_SESSION_RECEIVE_V2_STACK_KEY, "true")
                            .build())
                    .sessionReceiver()
                    .topicName(topicName)
                    .subscriptionName(subscriptionName)
                    .disableAutoComplete()
                    .maxAutoLockRenewDuration(maxSessionLockRenewal)
                    .buildAsyncClient();
        }
    }

    private Mono<SessionReceiver> sessionSupplier() {
        if (isTerminated.get()) {
            return Mono.error(SessionProviderException.forTermination());
        }
        return sessionAcceptor.acceptNextSession()
                .map(client -> {
                    return new SessionReceiver(pumpId, namespace, entityPath, client);
                })
                .onErrorMap(e -> {
                    isTerminated.set(true);
                    return SessionProviderException.forError(e);
                })
                .switchIfEmpty(Mono.defer(() -> {
                    isTerminated.set(true);
                    return Mono.error(SessionProviderException.forCompletion());
                }));
    }
}