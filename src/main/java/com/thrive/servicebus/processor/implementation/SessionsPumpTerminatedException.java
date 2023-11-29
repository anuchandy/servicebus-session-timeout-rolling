package com.thrive.servicebus.processor.implementation;

import com.azure.core.util.logging.ClientLogger;

/**
 * The error that {@link ConcurrentSessionsPumping} produces when its pumping gets terminated.
 */
final class SessionsPumpTerminatedException extends RuntimeException {
    private final String pumpId;
    private final String namespace;
    private final String entityPath;
    private SessionsPumpTerminatedException(String pumpId, String namespace, String entityPath,
        String message, Throwable terminationCause) {
        super(message, terminationCause);
        this.pumpId = pumpId;
        this.namespace = namespace;
        this.entityPath = entityPath;
    }
    static SessionsPumpTerminatedException forError(String pumpId, String namespace,
        String entityPath, Throwable reason) {
        return new SessionsPumpTerminatedException(pumpId, namespace,
                entityPath, "pumping#error-map", reason);
    }
    static SessionsPumpTerminatedException forCompletion(String pumpId, String namespace,
        String entityPath) {
        return new SessionsPumpTerminatedException(pumpId, namespace,
                entityPath, "pumping#reached-completion", null);
    }
    void log(ClientLogger logger, String message, boolean logError) {
        if (logError) {
            final SessionsPumpTerminatedException error = this;
            logger.atInfo()
                    .addKeyValue("pumpId", pumpId)
                    .addKeyValue("namespace", namespace)
                    .addKeyValue("entityPath", entityPath)
                    .log(message, error);
        } else {
            logger.atInfo()
                    .addKeyValue("pumpId", pumpId)
                    .addKeyValue("namespace", namespace)
                    .addKeyValue("entityPath", entityPath)
                    .log(message);
        }
    }
}