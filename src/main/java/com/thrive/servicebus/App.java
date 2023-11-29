package com.thrive.servicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.google.common.util.concurrent.Uninterruptibles;
import com.thrive.servicebus.processor.DispositionOperations;
import com.thrive.servicebus.processor.ServiceBusSessionProcessor;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public final class App {
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
    /**
     * The amount of time to wait for a message to receive from a session. If a message is not received from the session
     * within this time, then the {@link ServiceBusSessionProcessor} will close that session, and the Processor will proceed
     * with acquiring the next session.
     * <p>
     * IMPORTANT: USE A VALUE 3-5 SECONDS OR ABOVE AS THE TIMEOUT. Using low values such as 1 second often has adverse
     * effect under load leading to cancellation of the internal state preparations to obtain the message.
     * </p>
     */
    private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(5);
    /**
     * The amount of time the session lock needs to be keep renewed once the {@link ServiceBusSessionProcessor} acquires
     * a session. The Service Bus will close the session once this time expires. Note that if the SESSION_TIMEOUT triggers
     * before this duration, then the Processor will both proactively stops the renewal and closes the session.
     */
    private static final Duration SESSION_MAX_LOCK_RENEWAL = Duration.ofMinutes(10);
    /**
     * The maximum number of sessions that {@link ServiceBusSessionProcessor} should attempt to concurrently acquire and
     * receive messages from.
     * <p>
     * Change this based on the use case, measure and ensure the host (e.g. VM) has enough cores to handle this
     * concurrency load to avoid stall, crash and frequent timeout slowing down the overall progress.
     * </p>
     */
    private static final int maxConcurrentSessions = 50;

    public static void main( String[] args) {
        final ServiceBusSessionProcessor processor = new ServiceBusSessionProcessor(Config.CONNECTION_STRING,
                Config.TOPIC, Config.SUBSCRIPTION, maxConcurrentSessions, SESSION_MAX_LOCK_RENEWAL, SESSION_TIMEOUT,
                App::onMessage, Config.NAMESPACE);

        // Start pumping messages from the session enabled entity Config.SUBSCRIPTION under Config.TOPIC.
        final Disposable disposable = processor.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            RUNNING.set(false);
            disposable.dispose();
        }));

        // The above call to ServiceBusSessionProcessor::start triggers message pumping using demon threads and returns
        // control to main thread. The main thread (i.e, the non-demon thread) needs to be alive to stop JVM from exiting,
        // otherwise JVM exit will shut down the demon threads pumping messages.
        while (RUNNING.get()) {
            Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(30));
        }
    }

    /**
     * The application handler to process a message from a session.
     *
     * @param message the message.
     * @param dispositionOperations instance to request complete or abandon of the message.
     */
    private static void onMessage(ServiceBusReceivedMessage message, DispositionOperations dispositionOperations) {
        // handle the message and complete it.
        final boolean succeeded = Math.random() < 0.5;
        if (succeeded) {
            System.out.println("[" + Thread.currentThread().getName() + "] " + format(message, true));
            dispositionOperations.complete(message);
        } else {
            System.out.println("[" + Thread.currentThread().getName() + "] " + format(message, false));
            dispositionOperations.abandon(message);
        }
    }

    private static String format(ServiceBusReceivedMessage message, boolean succeeded) {
        return String.format("Message.SessionId: %s Message.SequenceNumber: %s Completed: %b", message.getSessionId(), message.getSequenceNumber(), succeeded);
    }
}
