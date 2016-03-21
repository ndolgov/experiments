package net.ndolgov.sqstest;

import net.ndolgov.sqstest.AsyncSqsClient.AsyncSqsClientCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public final class MessageHandler implements Handler {
    private final ExecutorService executor;

    private final AtomicInteger jobInProgressCounter = new AtomicInteger();

    private final int visibilityTimeout;

    private final int totalCapacity;

    public MessageHandler(int visibilityTimeout, ExecutorService executor) {
        this.visibilityTimeout = visibilityTimeout;
        this.executor = executor;

        this.totalCapacity = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public void handle(String message, AsyncSqsClientCallback callback) {
        jobInProgressCounter.incrementAndGet();

        executor.submit((Runnable) () -> {
            try {
                final long startedAt = System.currentTimeMillis();

                // todo process message

                callback.onSuccess("");
            } catch (Exception e) {
                callback.onFailure(e.getMessage());
            } finally {
                jobInProgressCounter.decrementAndGet();
            }
        });
    }

    @Override
    public int getRemainingCapacity() {
        return Math.max(totalCapacity - jobInProgressCounter.get(), 0);
    }

    @Override
    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }

}
