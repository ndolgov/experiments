package net.ndolgov.grpctest.plain;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.ndolgov.grpctest.api.TestServiceAProto.TestRequestA;
import net.ndolgov.grpctest.api.TestServiceAProto.TestResponseA;
import net.ndolgov.grpctest.api.TestServiceBProto;
import net.ndolgov.grpctest.api.TestServiceBProto.TestResponseB;
import net.ndolgov.grpctest.api.TestServiceAGrpc;
import net.ndolgov.grpctest.api.TestServiceAGrpc.TestServiceAFutureStub;
import net.ndolgov.grpctest.api.TestServiceBGrpc;
import net.ndolgov.grpctest.api.TestServiceBGrpc.TestServiceBFutureStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Create GRPC transport to a given "host:port" destination.<br>
 * For every (request, callback) pair, call the callback on a given executor when a response is received.
 *
 * todo support TLS (http://www.grpc.io/docs/guides/auth.html)
 */
public final class GrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);

    private final ManagedChannel channel;

    private final ExecutorService executor;

    private final TestServiceAFutureStub stubA;

    private final TestServiceBFutureStub stubB;

    public GrpcClient(String host, int port, ExecutorService executor) {
        this.executor = executor;
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
        this.stubA = TestServiceAGrpc.newFutureStub(channel);
        this.stubB = TestServiceBGrpc.newFutureStub(channel);
    }

    private TestServiceAFutureStub serviceA() {
        return stubA;
    }

    private TestServiceBFutureStub serviceB() {
        return stubB;
    }

    void callA(TestRequestA request, FutureCallback<TestResponseA> callback) {
        logger.info("Sending request A: " + request);
        callRpc(serviceA().process(request), callback);
    }

    public void callB(TestServiceBProto.TestRequestB request, FutureCallback<TestResponseB> callback) {
        logger.info("Sending request B: " + request);
        callRpc(serviceB().process(request), callback);
    }

    private <T> void callRpc(ListenableFuture<T> future, FutureCallback<T> callback) {
        Futures.addCallback(future, callback, executor);
    }

    public void close() {
        try {
            logger.info("Closing client");

            executor.shutdown();
            channel.shutdown().awaitTermination(5_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down");
        }
    }
}
