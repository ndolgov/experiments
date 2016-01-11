package net.ndolgov.thriftrpctest;

import net.ndolgov.thriftrpctest.api.TestRequest2;
import net.ndolgov.thriftrpctest.api.TestResponse2;
import net.ndolgov.thriftrpctest.api.TestService2;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Asynchronous RPC call handler illustrating how to process requests on a dedicated thread pool and
 * reply asynchronously once the future is finished.
 */
public final class Handler2 implements TestService2.AsyncIface {
    private static final Logger logger = LoggerFactory.getLogger(Handler2.class);

    public static final String RESULT = "RESULT2";

    private final Executor executor;

    public Handler2(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void process(TestRequest2 request, AsyncMethodCallback callback) {
        logger.info("Processing: " + request);

        final CompletableFuture<String> future = CompletableFuture.supplyAsync(
            () -> {
                return "RESULT2"; // todo this is where actual time-consuming processing would be
            },
            executor);

        future.whenComplete((result, e) -> {
            if (e == null) {
                callback.onComplete(new TestResponse2().setSuccess(true).setResult(result).setRequestId(request.getRequestId()));
            } else {
                callback.onComplete(new TestResponse2().setSuccess(false).setRequestId(request.getRequestId()));
            }
        });
    }
}
