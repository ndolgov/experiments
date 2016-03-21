package net.ndolgov.sqstest;

import net.ndolgov.sqstest.AsyncSqsClient.AsyncSqsClientCallback;

public interface Handler {
    /**
     * Process a message asynchronously on a thread other than the one calling this method
     * @param message message
     * @param callback message processing status listener
     */
    void handle(String message, AsyncSqsClientCallback callback);

    /**
     * @return how many messages this handler can process in one batch
     */
    default int getRemainingCapacity() {
        return 1;
    }

    /**
     * @return AWS SQS visibility timeout for this handler, [sec]
     */
    default int getVisibilityTimeout() {
        return 300;
    }
}
