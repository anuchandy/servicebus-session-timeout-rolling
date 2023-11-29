package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class IdleTimer {
    private static final Duration CANCEL = Duration.ofDays(365);
    private final AtomicBoolean monoObtained = new AtomicBoolean(false);
    private final Sinks.Many<Duration> timerSink = Sinks.many().multicast().onBackpressureBuffer();
    private final ClientLogger logger;
    private final Duration timeout;

    IdleTimer(ClientLogger logger, Duration timeout) {
        this.logger = logger;
        this.timeout = Objects.requireNonNull(timeout);
    }

    Mono<Void> timeout() {
        final Mono<Void> canObtainOnce = Mono.fromRunnable(() -> {
            if (monoObtained.getAndSet(true)) {
                throw new IllegalStateException("timeoutMono can be obtained only once.");
            }
        });

        final Mono<Void> movingTimer = Flux.switchOnNext(timerSink.asFlux()
                        .map(d -> d == CANCEL ? Mono.never() : Mono.delay(d)))
                .take(1)
                .doOnNext(__ -> logger.atInfo().log("Did not a receive message within timeout."))
                .then();

        return canObtainOnce.then(movingTimer)
                .doOnSubscribe(__ -> {
                    start();
                });
    }

    void start() {
        timerSink.emitNext(timeout, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    void cancel() {
        timerSink.emitNext(CANCEL, Sinks.EmitFailureHandler.FAIL_FAST);
    }
}

