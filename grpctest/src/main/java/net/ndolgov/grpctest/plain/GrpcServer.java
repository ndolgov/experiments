package net.ndolgov.grpctest.plain;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Create a GRPC server for given request handlers and bind it to provided "host:port".
 */
public final class GrpcServer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class);
    private final ExecutorService executor;
    private final Server server;
    private final int port;

    public GrpcServer(String hostname, int port, Collection<BindableService> services, ExecutorService executor) {
        this.port = port;
        this.executor = executor;

        final ServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress(hostname, port));
        services.forEach(builder::addService);
        this.server = builder.executor(executor).build();
    }

    public void start() {
        try {
            server.start();
            logger.info("Started " + this);
        } catch (Exception e) {
            throw new RuntimeException("Could not start server", e);
        }
    }

    public void stop() {
        try {
            logger.info("Stopping " + this);

            executor.shutdown();

            server.shutdown();

            logger.info("Stopped " + this);
        } catch (Exception e) {
            logger.warn("Interrupted while shutting down " + this);
        }
    }

    @Override
    public String toString() {
        return "{GrpcServer:port=" + port + "}";
    }
}
