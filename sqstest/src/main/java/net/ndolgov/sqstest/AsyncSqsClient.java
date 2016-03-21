package net.ndolgov.sqstest;

import com.amazonaws.services.sqs.model.Message;

import java.util.List;
import java.util.function.Function;

public interface AsyncSqsClient {
    void receive(String queueUrl, int maxMessages, int visibilityTimeout, Function<List<Message>, Void> handler);

    void delete(String queueUrl, String handle);

    void renew(String queueUrl, String handle, int visibilityTimeout, Function<Void, Void> handler);

    void close();

    interface AsyncSqsClientCallback {
        void onSuccess(String handle);

        void onFailure(String message);
    }
}
