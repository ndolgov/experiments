package net.ndolgov.sqstest;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import org.slf4j.Logger;

import java.util.List;
import java.util.function.Function;

/**
 * Half-sync/half-async SQS request converter
 */
public final class AsyncSqsClientImpl implements AsyncSqsClient {
    private final static int MAX_NUMBER_OF_MESSAGES = 10; // AWS-enforced maximum number of messages for one request

    private final Logger logger;
    private final AmazonSQSAsync sqsClient;
    private final int waitTime;

    public AsyncSqsClientImpl(Logger logger, int waitTime) {
        this.logger = logger;
        this.waitTime = waitTime;
        this.sqsClient = new AmazonSQSAsyncClient();
    }

    @Override
    public void close() {
        sqsClient.shutdown();
    }

    @Override
    public void receive(String queueUrl, int maxMessages, int visibilityTimeout, Function<List<Message>, Void> handler) {
        final ReceiveMessageRequest request = new ReceiveMessageRequest().
            withQueueUrl(queueUrl).
            withMaxNumberOfMessages(Math.min(maxMessages, MAX_NUMBER_OF_MESSAGES)).
            withVisibilityTimeout(visibilityTimeout).
            withWaitTimeSeconds(waitTime);

        receive(handler, request);
    }

    private void receive(Function<List<Message>, Void> handler, ReceiveMessageRequest request) {
        sqsClient.receiveMessageAsync(request, new AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult>() {
            @Override
            public void onError(Exception e) {
                logger.error("Failed to poll queue: " + request.getQueueUrl(), e);
            }

            @Override
            public void onSuccess(ReceiveMessageRequest request, ReceiveMessageResult response) {
                handler.apply(response.getMessages());
            }
        });
    }

    @Override
    public void delete(String queueUrl, String handle) {
        final DeleteMessageRequest request = new DeleteMessageRequest(queueUrl, handle);

        sqsClient.deleteMessageAsync(request, new AsyncHandler<DeleteMessageRequest, Void>() {
            @Override
            public void onError(Exception e) {
                logger.error("Failed to delete message: " + handle + " from queue: " + queueUrl, e);
            }

            @Override
            public void onSuccess(DeleteMessageRequest request, Void unit) {
                logger.info("Deleted message: " + handle.substring(handle.length() - 10));
            }
        });
    }

    @Override
    public void renew(String queueUrl, String handle, int visibilityTimeout, Function<Void, Void> handler) {
        final ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(queueUrl, handle, visibilityTimeout);

        sqsClient.changeMessageVisibilityAsync(request, new AsyncHandler<ChangeMessageVisibilityRequest, Void>() {
            @Override
            public void onError(Exception e) {
                logger.error("Failed to change visibility of message: " + handle + " in queue: " + queueUrl, e);
            }

            @Override
            public void onSuccess(ChangeMessageVisibilityRequest request, Void unit) {
                handler.apply(null);
            }
        });
    }

    public void resolveQueueUrl(String absoluteQueueName, Function<String, Void> handler) {
        final GetQueueUrlRequest request = new GetQueueUrlRequest(absoluteQueueName);

        sqsClient.getQueueUrlAsync(request, new AsyncHandler<GetQueueUrlRequest, GetQueueUrlResult>() {
            @Override
            public void onError(Exception e) {
                logger.error("Failed to resolve queue name: " + absoluteQueueName, e);
            }

            @Override
            public void onSuccess(GetQueueUrlRequest request, GetQueueUrlResult response) {
                handler.apply(response.getQueueUrl());
            }
        });
    }
}