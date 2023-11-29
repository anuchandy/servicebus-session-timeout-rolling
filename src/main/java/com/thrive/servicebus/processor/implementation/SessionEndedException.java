package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;

/**
 * The error that {@link SessionReceiver} produces if it gets disconnected from the session.
 */
final class SessionEndedException extends RuntimeException {
    private final String sessionId;

    private SessionEndedException(String sessionId, String message, Throwable reason) {
        super(message, reason);
        this.sessionId = sessionId;
    }

    static SessionEndedException forError(String sessionId, Throwable reason) {
        return new SessionEndedException(sessionId, "session-errored", reason);
    }

    static SessionEndedException forCompletion(String sessionId) {
        return new SessionEndedException(sessionId, "session-completed", null);
    }

    void log(ClientLogger logger, String message, boolean logError) {
        if (logError) {
            final SessionEndedException error = this;
            logger.atInfo()
                    .addKeyValue("sessionId", sessionId)
                    .log(message, error);
        } else {
            logger.atInfo()
                    .addKeyValue("sessionId", sessionId)
                    .log(message);
        }
    }
}
