package net.ndolgov.grpctest.plain;

import io.grpc.stub.StreamObserver;
import net.ndolgov.grpctest.api.TestServiceAProto.TestRequestA;
import net.ndolgov.grpctest.api.TestServiceAProto.TestResponseA;
import net.ndolgov.grpctest.api.TestServiceAGrpc.TestServiceAImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Service A request handler
 */
public final class TestServiceAImpl extends TestServiceAImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TestServiceAImpl.class);

    private final Executor executor;

    public TestServiceAImpl(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void process(TestRequestA request, StreamObserver<TestResponseA> observer) {
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

    private static TestResponseA response(long requestId, String result) {
        return TestResponseA.newBuilder().
            setRequestId(requestId).
            setSuccess(true).
            setResult(result).
            build();
    }
}
