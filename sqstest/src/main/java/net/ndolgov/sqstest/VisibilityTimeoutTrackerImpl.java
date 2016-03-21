package net.ndolgov.sqstest;

import org.slf4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class VisibilityTimeoutTrackerImpl implements VisibilityTimeoutTracker {
    private final Logger logger;

    private final ScheduledExecutorService scheduler;

    private final MessageRepository repository;

    private final AsyncSqsClient sqsClient;

    public VisibilityTimeoutTrackerImpl(Logger logger, MessageRepository repository, AsyncSqsClient sqsClient, ScheduledExecutorService scheduler) {
        this.logger = logger;
        this.repository = repository;
        this.sqsClient = sqsClient;
        this.scheduler = scheduler;
    }

    @Override
    public Future<?> track(String handle, int timeout) {
        return scheduler.schedule(() -> {
                logger.info("Extending visibility of message: " + handle + " by seconds: " + timeout);

                final String queueUrl = repository.get(handle);
                if (queueUrl == null) {
                    logger.info("Message was already processed: " + handle);
                } else {
                    sqsClient.renew(queueUrl, handle, timeout, unit -> {
                        track(handle, timeout);
                        return null;
                    });
                }
            },
            timeout,
            TimeUnit.SECONDS);
    }
}