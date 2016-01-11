package net.ndolgov.thriftrpctest;

import net.ndolgov.thriftrpctest.api.TestRequest;
import net.ndolgov.thriftrpctest.api.TestRequest2;
import net.ndolgov.thriftrpctest.api.TestResponse;
import net.ndolgov.thriftrpctest.api.TestResponse2;
import net.ndolgov.thriftrpctest.api.TestService;
import net.ndolgov.thriftrpctest.api.TestService2;

import org.apache.thrift.async.AsyncMethodCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public final class ServerTest {
    private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
    private static final int PORT = 20000;
    private static final long REQUEST_ID = 123;
    private static final String HOSTNAME = "localhost";

    private final static ServiceDefinition<TestService.AsyncIface> DEF = new ServiceDefinition<>(
        TestService.AsyncIface.class,
        "TestSvc",
        TestService.AsyncProcessor.class,
        TestService.AsyncClient.Factory.class);

    private final static ServiceDefinition<TestService2.AsyncIface> DEF2 = new ServiceDefinition<>(
        TestService2.AsyncIface.class,
        "TestSvc2",
        TestService2.AsyncProcessor.class,
        TestService2.AsyncClient.Factory.class);

    @Test
    public void testTwoEndPoints() throws Exception {
        new Thread(() -> {
            final ArrayList<ServiceDefinition> endpoints = new ArrayList<>();
            endpoints.add(DEF);
            endpoints.add(DEF2);

            final Server server = new Server(logger, handlerFactory());
            server.init(PORT, endpoints, Executors.newSingleThreadExecutor());
        }).start();

        final ClientFactory clientFactory = new MultiplexedClientFactory(PORT);
        
        final TestService.AsyncIface client = clientFactory.create(DEF, HOSTNAME);
        final TestResponse response = callTestService(client);
        logger.info("Received: " + response);
        assertEquals(response.getResult(), Handler.RESULT);

        final TestService2.AsyncIface client2 = clientFactory.create(DEF2, HOSTNAME);
        final TestResponse2 response2 = callTestService2(client2);
        logger.info("Received: " + response2);
        assertEquals(response2.getResult(), Handler2.RESULT);

        clientFactory.close();
    }

    private static TestResponse callTestService(TestService.AsyncIface client) throws Exception {
        final CompletableFuture<TestResponse> result = new CompletableFuture<>();

        client.process(request(), new AsyncMethodCallback<TestService.AsyncClient.process_call>() {
            @Override
            public void onComplete(TestService.AsyncClient.process_call response) {
                extract(response);
            }

            @Override
            public void onError(Exception e) {
                logger.error("RPC call failed ", e);
            }

            private void extract(TestService.AsyncClient.process_call response) {
                try {
                    result.complete(response.getResult());
                } catch (Exception e) {
                    logger.error("Failed to extract response from: " + response, e);
                }
            }
        });

        return result.get(5, TimeUnit.SECONDS);
    }

    private static TestResponse2 callTestService2(TestService2.AsyncIface client) throws Exception {
        final CompletableFuture<TestResponse2> result = new CompletableFuture<>();

        client.process(request2(), new AsyncMethodCallback<TestService2.AsyncClient.process_call>() {
            @Override
            public void onComplete(TestService2.AsyncClient.process_call response) {
                extract(response);
            }

            @Override
            public void onError(Exception e) {
                logger.error("RPC call failed ", e);
            }

            private void extract(TestService2.AsyncClient.process_call response) {
                try {
                    result.complete(response.getResult());
                } catch (Exception e) {
                    logger.error("Failed to extract response from: " + response, e);
                }
            }
        });

        return result.get(5, TimeUnit.SECONDS);
    }

    private static TestRequest request() {
        return new TestRequest().setRequestId(REQUEST_ID);
    }


    private static TestRequest2 request2() {
        return new TestRequest2().setRequestId(REQUEST_ID);
    }


    private static HandlerFactory handlerFactory() {
        return new HandlerFactory() {
            private final Executor executor = Executors.newFixedThreadPool(2);

            @Override
            public <T> T handler(ServiceDefinition<T> definition) {
                if (definition.getName().equals(DEF.getName())) {
                    return (T) new Handler(executor);
                } else if (definition.getName().equals(DEF2.getName())) {
                    return (T) new Handler2(executor);
                } else {
                    throw new IllegalArgumentException(definition.getName());
                }
            }
        };
    }

}
