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
    private final AtomicBoolean timerObtained = new AtomicBoolean(false);
    private final Sinks.Many<Duration> timerSink = Sinks.many().multicast().onBackpressureBuffer();
    private final ClientLogger logger;
    private final int rollerId;
    private final Scheduler timerScheduler;
    private final Duration timeout;

    SessionIdleTimer(ClientLogger logger, int rollerId, Scheduler timerScheduler, Duration timeout) {
        this.logger = Objects.requireNonNull(logger);
        this.rollerId = rollerId;
        this.timerScheduler = Objects.requireNonNull(timerScheduler);
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
        final Mono<Void> assertNotObtained = Mono.fromRunnable(() -> {
            if (timerObtained.getAndSet(true)) {
                throw new IllegalStateException("timeout Mono can be subscribed only once.");
            }
        });

        final Mono<Void> obtainTimer = Flux.switchOnNext(timerSink.asFlux().map(d -> {
                return d == CANCEL ? Mono.never() : Mono.delay(d, timerScheduler);
            }))
            .take(1)
            .doOnNext(__ -> logger.atInfo()
                    .addKeyValue("rollerId", rollerId)
                    .log("Did not a receive message from the session within timeout {}.", timeout))
            .then();

        return assertNotObtained.then(obtainTimer)
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
     * <p>
     * It is important for caller to ensure that start and cancel calls never overlaps, i.e, calls must happens serially
     * to adhere to reactive serialized access.
     * </p>
     */
    void start() {
        timerSink.emitNext(timeout, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    /**
     * Cancel the timer that is currently running (which was started by @link SessionIdleTimer#start()}).
     * <p>
     * It is important for caller to ensure that start and cancel calls never overlaps, i.e, calls must happens serially
     * to adhere to reactive serialized access.
     * </p>
     */
    void cancel() {
        timerSink.emitNext(CANCEL, Sinks.EmitFailureHandler.FAIL_FAST);
    }
}

