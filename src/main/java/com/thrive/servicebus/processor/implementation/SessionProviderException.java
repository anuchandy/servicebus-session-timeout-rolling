package com.thrive.servicebus.processor.implementation;

/**
 * The error that {@link SessionProvider#mono()} produces if it is subscribed after the provider termination.
 */
final class SessionProviderException extends RuntimeException {
    private SessionProviderException(String message, Throwable reason) {
        super(message, reason);
    }

    static SessionProviderException forError(Throwable reason) {
        return new SessionProviderException("session-provider-errored", reason);
    }

    static SessionProviderException forCompletion() {
        return new SessionProviderException("session-provider-completed", null);
    }

    static SessionProviderException forTermination() {
        return new SessionProviderException("session-provider-terminated", null);
    }
}