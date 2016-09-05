package net.ndolgov.grpctest.plain;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.BindableService;
import net.ndolgov.grpctest.api.TestServiceAProto.TestRequestA;
import net.ndolgov.grpctest.api.TestServiceAProto.TestResponseA;
import net.ndolgov.grpctest.api.TestServiceBProto.TestRequestB;
import net.ndolgov.grpctest.api.TestServiceBProto.TestResponseB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class AsyncGrpcCallTest {
    private static final Logger logger = LoggerFactory.getLogger(AsyncGrpcCallTest.class);
    private static final int PORT = 20000;
    private static final long REQUEST_ID = 123;
    private static final String HOSTNAME = "127.0.0.1";
    private static final int THREADS = 4;
    private static final String RESULT = "RESULT";

    @Test
    public void testTwoEndPoints() throws Exception {
        final GrpcServer server = new GrpcServer(HOSTNAME, PORT, services(), serverExecutor());
        server.start();


        final GrpcClient client = new GrpcClient(HOSTNAME, PORT, clientExecutor());

        try {
            final CountDownLatch latch = new CountDownLatch(2);
            invokeRpc(client, latch);
            assertTrue(latch.await(5_000, TimeUnit.MILLISECONDS));
        } finally {
            client.close();

            server.stop();
        }
    }

    private static List<BindableService> services() {
        final Executor executor = handlerExecutor();
        return newArrayList(new TestServiceAImpl(executor), new TestServiceBImpl(executor));
    }

    private void invokeRpc(GrpcClient client, final CountDownLatch latch) {
        client.callA(requestA(), new Callback<TestResponseA>(latch) {
            @Override
            public void onSuccess(TestResponseA response) {
                assertEquals(response.getRequestId(), REQUEST_ID);
                assertEquals(response.getSuccess(), true);
                assertEquals(response.getResult(), RESULT);
                super.onSuccess(response);
            }
        });

        client.callB(requestB(), new Callback<TestResponseB>(latch) {
            @Override
            public void onSuccess(TestResponseB response) {
                assertEquals(response.getRequestId(), REQUEST_ID);
                assertEquals(response.getSuccess(), true);
                assertEquals(response.getResult(), RESULT);
                super.onSuccess(response);
            }
        });
    }

    private static TestRequestA requestA() {
        return TestRequestA.newBuilder().setRequestId(REQUEST_ID).build();
    }

    private static TestRequestB requestB() {return TestRequestB.newBuilder().setRequestId(REQUEST_ID).build();}

    private static ExecutorService clientExecutor() {
        return Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-client-%d").build());
    }

    private static ExecutorService serverExecutor() {
        return Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-server-%d").build());
    }

    private static ExecutorService handlerExecutor() {
        return Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("server-%d").build());
    }

    private abstract static class Callback<T> implements FutureCallback<T> {
        private final CountDownLatch latch;

        private Callback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onSuccess(T response) {
            logger.info("Processing response: " + response);
            latch.countDown();
        }

        @Override
        public final void onFailure(Throwable t) {
            logger.error("RPC call failed ", t);
        }
    }
}
