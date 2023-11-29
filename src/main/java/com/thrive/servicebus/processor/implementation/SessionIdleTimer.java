package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A type to support session idle timeout.
 */
final class SessionIdleTimer {
    private static final Duration CANCEL = Duration.ofDays(365);
    private final AtomicBoolean monoObtained = new AtomicBoolean(false);
    private final Sinks.Many<Duration> timerSink = Sinks.many().multicast().onBackpressureBuffer();
    private final ClientLogger logger;
    private final String sessionId;
    private final Scheduler timeoutScheduler;
    private final Duration timeout;

    SessionIdleTimer(ClientLogger logger, String sessionId, Scheduler timeoutScheduler, Duration timeout) {
        this.logger = Objects.requireNonNull(logger);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler);
        this.timeout = Objects.requireNonNull(timeout);
    }

    /**
     * Gets the mono that terminates once the timeout timer expires.
     * <p>
     *  The timeout timer is started for the first time when this mono is subscribed. If desired, the timer can be
     *  canceled before the expiration by calling {@link SessionIdleTimer#cancel()}. Later the timer can be started
     *  again by calling {@link SessionIdleTimer#start()}.
     * </p>
     *
     * @return the timeout mono.
     */
    Mono<Void> timeout() {
        final Mono<Void> canObtainOnce = Mono.fromRunnable(() -> {
            if (monoObtained.getAndSet(true)) {
                throw new IllegalStateException("timeout Mono can be subscribed only once.");
            }
        });

        final Mono<Void> timeoutTimer = Flux.switchOnNext(timerSink.asFlux().map(d -> {
                return d == CANCEL ? Mono.never() : Mono.delay(d, timeoutScheduler);
            }))
            .take(1)
            .doOnNext(__ -> logger.atInfo().log("Did not a receive message from the session {} within timeout {}.", sessionId, timeout))
            .then();

        return canObtainOnce.then(timeoutTimer)
                .doOnSubscribe(__ -> {
                    start();
                });
    }

    /**
     * Starts the running of the timer that when expires i.e, when the timeout duration elapses, terminates the
     * {@link SessionIdleTimer#timeout()} mono.
     * <p>
     * If the {@link SessionIdleTimer#cancel()} is called before the timer expiration then timeout mono will continue
     * to stay in non-terminated (alive) state.
     * </p>
     */
    void start() {
        timerSink.emitNext(timeout, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    /**
     * Cancel the timer that is currently running (which was started by @link SessionIdleTimer#start()}).
     */
    void cancel() {
        timerSink.emitNext(CANCEL, Sinks.EmitFailureHandler.FAIL_FAST);
    }
}

