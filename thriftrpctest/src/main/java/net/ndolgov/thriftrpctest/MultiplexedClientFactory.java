package net.ndolgov.thriftrpctest;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Establish a connection to a given service instance
 */
public final class MultiplexedClientFactory implements ClientFactory {
    private final TAsyncClientManager manager;
    private final TProtocolFactory factory;
    private final int port;

    public MultiplexedClientFactory(int port) {
        this.port = port;

        factory = new TBinaryProtocol.Factory();

        try {
            manager = new TAsyncClientManager();
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create client manager", e);
        }
    }

    @Override
    public <T> T create(ServiceDefinition<T> definition, String hostname) {
        try {
            final TProtocolFactory pfactory = new TMultiplexedProtocolFactory(factory, definition.getName()); // todo cache?
            return (T) definition.clientFactory(pfactory, manager).getAsyncClient(new TNonblockingSocket(hostname, port));
        } catch (Exception e) {
            throw new RuntimeException("Could not create client to: " + hostname + " for service: " + definition, e);
        }
    }

    @Override
    public void close() {
        manager.stop();
    }

    private final static class TMultiplexedProtocolFactory implements TProtocolFactory {
        private final TProtocolFactory factory;
        private final String name;

        public TMultiplexedProtocolFactory(TProtocolFactory factory, String name) {
            this.factory = factory;
            this.name = name;
        }

        public TProtocol getProtocol(TTransport transport) {
            return new TMultiplexedProtocol(factory.getProtocol(transport), name);
        }
    }
}
