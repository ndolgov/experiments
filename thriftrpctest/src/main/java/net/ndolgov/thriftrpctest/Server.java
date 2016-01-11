package net.ndolgov.thriftrpctest;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Thrift server capable of running multiple Thrift service handlers on the same network endpoint
 */
public final class Server {
    private final Logger logger;
    private final HandlerFactory factory;

    public Server(Logger logger, HandlerFactory factory) {
        this.logger = logger;
        this.factory = factory;
    }

    public void init(int port, List<ServiceDefinition> endpoints, ExecutorService executor) {
        start(create(port, endpoints, executor));
    }

    private void start(TServer server) {
        logger.info("Starting Thrift server");

        server.serve();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Stopping Thrift server");
                server.stop();
            }
        });
    }

    private TServer create(int port, List<ServiceDefinition> endpoints, ExecutorService executor) {

        try {
            logger.info("Configuring Thrift server on port: " + port);

            final THsHaServer.Args args = new THsHaServer.Args(new TNonblockingServerSocket(port)).
                processor(multiplexedProcessor(endpoints)).
                executorService(executor);

            return new TNonblockingServer(args);
        } catch (TTransportException e) {
            throw new IllegalArgumentException("Failed to create Thrift server", e);
        }
    }

    private TMultiplexedAsyncProcessor multiplexedProcessor(List<ServiceDefinition> endpoints) {
        final TMultiplexedAsyncProcessor processor = new TMultiplexedAsyncProcessor();

        for (ServiceDefinition def : endpoints) {
            logger.info("Registering Thrift processor for service: " + def.getName());
            final Object impl = factory.handler(def);
            processor.registerProcessor(def.getName(), def.buildProcessor(impl), impl);
        }

        return processor;
    }
}
