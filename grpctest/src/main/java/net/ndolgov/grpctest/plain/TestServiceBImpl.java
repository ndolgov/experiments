package net.ndolgov.grpctest.plain;

import io.grpc.stub.StreamObserver;
import net.ndolgov.grpctest.api.TestServiceBProto.TestRequestB;
import net.ndolgov.grpctest.api.TestServiceBProto.TestResponseB;
import net.ndolgov.grpctest.api.TestServiceBGrpc.TestServiceBImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Service B request handler
 */
public final class TestServiceBImpl extends TestServiceBImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TestServiceBImpl.class);

    private final Executor executor;

    public TestServiceBImpl(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void process(TestRequestB request, StreamObserver<TestResponseB> observer) {
        logger.info("Processing request: " + request);

        final CompletableFuture<String> future = CompletableFuture.supplyAsync(
            () -> {
                logger.info("Computing result");  // todo this is where actual time-consuming processing would be
                return "RESULT";
            },
            executor);

        future.whenComplete((result, e) -> {
            if (e == null) {
                observer.onNext(response(request.getRequestId(), result));
                observer.onCompleted();
            } else {
                observer.onError(e);
            }
        });
    }

    private static TestResponseB response(long requestId, String result) {
        return TestResponseB.newBuilder().
            setRequestId(requestId).
            setSuccess(true).
            setResult(result).
            build();
    }
}
