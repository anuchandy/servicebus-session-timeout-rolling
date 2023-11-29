package com.thrive.servicebus.processor.implementation;

/**
 * Type indicating reason for termination of pumping in various levels.
 */
enum TerminalSignalType {
    COMPLETED,
    ERRORED,
    CANCELED,
}
