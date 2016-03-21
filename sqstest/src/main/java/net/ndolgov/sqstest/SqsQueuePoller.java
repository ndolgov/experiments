package net.ndolgov.sqstest;

import com.amazonaws.services.sqs.model.Message;

public final class SqsQueuePoller {
    private static final double SAFETY_MARGIN = 0.9;
    private final AsyncSqsClient sqsClient;
    private final MessageRepository repository;
    private final VisibilityTimeoutTracker tracker;

    public SqsQueuePoller(AsyncSqsClient sqsClient, MessageRepository repository, VisibilityTimeoutTracker tracker) {
        this.sqsClient = sqsClient;
        this.repository = repository;
        this.tracker = tracker;
    }

    public void poll(String queueUrl, Handler handler) {
        final int maxMessages = handler.getRemainingCapacity();
        if (maxMessages > 0) {
            sqsClient.receive(queueUrl, maxMessages, handler.getVisibilityTimeout(), messages -> {
                for (Message message : messages) {
                    handle(queueUrl, message, handler);
                }

                return null;
            });
        }
    }

    private void handle(String queueUrl, Message message, Handler handler) {
        final String handle = message.getReceiptHandle();

        repository.put(message.getReceiptHandle(), queueUrl);

        tracker.track(handle, (int) (handler.getVisibilityTimeout() * SAFETY_MARGIN));

        handler.handle(message.getBody(), new AsyncSqsClient.AsyncSqsClientCallback() {
            @Override
            public void onSuccess(String handle) {
                sqsClient.delete(repository.remove(handle), handle);
            }

            @Override
            public void onFailure(String handle) {
                sqsClient.delete(repository.remove(handle), handle);
            }
        });
    }
}
