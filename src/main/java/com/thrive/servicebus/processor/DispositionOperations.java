package com.thrive.servicebus.processor;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

/**
 * A contract describing the disposition operations on session message.
 */
public interface DispositionOperations {
    void complete(ServiceBusReceivedMessage message);
    void abandon(ServiceBusReceivedMessage message);
}
