package net.ndolgov.sqstest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Only an example of wiring up, the test itself will fail
 */
public final class SqsQueuePollerTest {
    private static final Logger logger = LoggerFactory.getLogger(SqsQueuePollerTest.class);
    private static final int VISIBILITY_TUMEOUT = 900;
    private static final String QUEUE_URL = "http://sqs.us-east-1.amazonaws.com/123456789012/queue2";

    @Test
    public void testOneQueuePollingSetup() {
        final AsyncSqsClientImpl sqsClient = new AsyncSqsClientImpl(logger, VISIBILITY_TUMEOUT);
        final ConcurrentMapMessageRepository repository = new ConcurrentMapMessageRepository();

        final SqsQueuePoller poller = new SqsQueuePoller(
            sqsClient,
            repository,
            new VisibilityTimeoutTrackerImpl(logger, repository, sqsClient, newSingleThreadScheduledExecutor()));

        poller.poll(
            QUEUE_URL,
            new MessageHandler(VISIBILITY_TUMEOUT, newSingleThreadExecutor()));

        sqsClient.close();
    }
}
