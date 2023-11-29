package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.thrive.servicebus.processor.DispositionOperations;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;

/**
 * A type that connects to at most maxConcurrentSessions number of sessions and serially pump messages from each session
 * but concurrently across the sessions. Each {@link SessionReceiver} connected to a session is managed by
 * {@link RollingSessionReceiver}, which means there are maxConcurrentSessions number of {@link RollingSessionReceiver}
 * instances.
 * <p>
 * The pumping across sessions starts once the begin API is called and the caller subscribes to the Mono this API returned.
 * </p>
 * <p>
 * If one of the {@link RollingSessionReceiver} is unable to roll to a new session, then all the concurrent pumping in
 * {@link ConcurrentSessionsPumping} will be terminated and Mono that the begin API returned gets terminated by signaling
 * {@link SessionsPumpTerminatedException} error. The {@link RetryableConcurrentSessionsPumping} type will listen to such
 * error and restarts the pumping by creating new {@link ConcurrentSessionsPumping}.
 * </p>
 */
final class ConcurrentSessionsPumping {
    private final AtomicReference<State> state = new AtomicReference<>(State.EMPTY);
    private final String pumpId;
    private final ClientLogger logger;
    private final int maxConcurrentSessions;
    private final Duration sessionTimeout;
    private final BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage;
    private final String namespace;
    private final String entityPath;
    private final SessionProvider sessionProvider;

    ConcurrentSessionsPumping(String pumpId, int maxConcurrentSessions, Duration sessionTimeout,
        BiConsumer<ServiceBusReceivedMessage, DispositionOperations> onMessage,
        String namespace, String entityPath, SessionProvider sessionProvider) {
        this.pumpId = pumpId;
        final Map<String, Object> loggingContext = new HashMap<>(3);
        loggingContext.put("pumpId", pumpId);
        loggingContext.put("namespace", namespace);
        loggingContext.put("entityPath", entityPath);
        this.logger = new ClientLogger(ConcurrentSessionsPumping.class, loggingContext);
        this.maxConcurrentSessions = maxConcurrentSessions;
        this.sessionTimeout = sessionTimeout;
        this.onMessage = onMessage;
        this.namespace = namespace;
        this.entityPath = entityPath;
        this.sessionProvider = sessionProvider;
    }

    Mono<Void> begin() {
        final Mono<Void> pumping = Mono.usingWhen(createResource(), resource -> beginPumping(resource),
                resource -> terminate(resource, TerminalSignalType.COMPLETED),
                (resource, e) -> terminate(resource, TerminalSignalType.ERRORED),
                (resource) -> terminate(resource, TerminalSignalType.CANCELED));

        return pumping
                .onErrorMap(e -> SessionsPumpTerminatedException.forError(pumpId, namespace, entityPath, e))
                .then(Mono.error(() -> SessionsPumpTerminatedException.forCompletion(pumpId, namespace, entityPath)));
    }

    private Mono<Resource> createResource() {
        return Mono.fromSupplier(() -> {
            throwIfTerminatedOrInitialized();
            final ArrayList<RollingSessionReceiver> rollingReceivers = new ArrayList<>(maxConcurrentSessions);
            for (int rollerId = 1; rollerId <= maxConcurrentSessions; rollerId++) {
                final RollingSessionReceiver rollingReceiver = new RollingSessionReceiver(rollerId, sessionTimeout,
                        onMessage, sessionProvider);
                rollingReceivers.add(rollingReceiver);
            }
            if (!state.compareAndSet(State.EMPTY, State.INITIALIZED)) {
                rollingReceivers.clear();
                throwIfTerminatedOrInitialized();
            }
            final Scheduler pumpScheduler = Schedulers.newBoundedElastic(DEFAULT_BOUNDED_ELASTIC_SIZE,
                    DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "pumping-" + pumpId);
            final Scheduler timerScheduler = Schedulers.newParallel("timer-" + pumpId, Schedulers.DEFAULT_POOL_SIZE);
            return new Resource(rollingReceivers, pumpScheduler, timerScheduler);
        });
    }

    private Mono<Void> beginPumping(Resource resource) {
        final List<RollingSessionReceiver> rollingReceivers = resource.getReceivers();
        final Scheduler pumpScheduler = resource.getPumpScheduler();
        final Scheduler timeoutScheduler = resource.getTimerScheduler();

        final List<Mono<Void>> pumpingList = new ArrayList<>(rollingReceivers.size());
        for (RollingSessionReceiver rollingReceiver : rollingReceivers) {
            pumpingList.add(rollingReceiver.begin(pumpScheduler, timeoutScheduler));
        }
        final Mono<Void> pumping = Mono.when(pumpingList);
        return pumping;
    }

    private Mono<Void> terminate(Resource resource, TerminalSignalType signalType) {
        final State s = state.getAndSet(State.TERMINATED);
        if (s != State.TERMINATED) {
            logger.atInfo().log("Pump terminated. signal:" + signalType);
            resource.getPumpScheduler().dispose();
            resource.getTimerScheduler().dispose();
        }
        return Mono.empty();
    }


    private void throwIfTerminatedOrInitialized() {
        final State s = state.get();
        if (s == State.TERMINATED) {
            throw logger.atVerbose().log(new IllegalStateException("Cannot invoke begin() once terminated."));
        }
        if (s != State.EMPTY) {
            throw logger.atVerbose().log(new IllegalStateException("Cannot invoke begin() more than once."));
        }
    }

    /**
     * The various resources that {@link ConcurrentSessionsPumping} and child types uses internally pump messages
     * from sessions.
     */
    private static class Resource {
        private final List<RollingSessionReceiver> rollingSessionReceivers;
        private final Scheduler pumpScheduler;
        private final Scheduler timerScheduler;

        Resource(List<RollingSessionReceiver> rollingSessionReceivers, Scheduler pumpScheduler, Scheduler timerScheduler) {
            this.rollingSessionReceivers = rollingSessionReceivers;
            this.pumpScheduler = pumpScheduler;
            this.timerScheduler = timerScheduler;
        }

        /**
         * Gets the list of {@link RollingSessionReceiver}, where each of them serially pump messages from a specific session.
         *
         * @return the rolling receivers.
         */
        List<RollingSessionReceiver> getReceivers() {
            return rollingSessionReceivers;
        }

        /**
         * Gets the {@link Scheduler} that hosts threads to deliver (pump) messages to application message handler.
         *
         * @return the scheduler to deliver (pump) messages.
         */
        Scheduler getPumpScheduler() {
            return pumpScheduler;
        }

        /**
         * Gets the {@link Scheduler} that hosts timer threads for {@link SessionIdleTimer} to time out a session
         * when it goes idle.
         *
         * @return the timer threads scheduler.
         */
        Scheduler getTimerScheduler() {
            return timerScheduler;
        }
    }

    /**
     * The internal state of {@link ConcurrentSessionsPumping}.
     */
    private enum State {
        EMPTY, INITIALIZED, TERMINATED
    }
}