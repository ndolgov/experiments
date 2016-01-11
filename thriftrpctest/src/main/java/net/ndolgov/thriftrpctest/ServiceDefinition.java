package net.ndolgov.thriftrpctest;

import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * Definition of an asynchronous Thrift service. Every instance is supposed to be shared between client and server.
 */
public final class ServiceDefinition<IFACE> {
    /** Thrift-generated service interface */
    private final Class<IFACE> ifaceClass;

    /** Service identifier used for multiplexing */
    private final String name;

    /** Thrift-generated service call handler class */
    private final Class<? extends TBaseAsyncProcessor> processorClass;

    /** Thrift-generated service client factory class */
    private final Class<?> clientFactoryClass;

    public ServiceDefinition(
        Class<IFACE> ifaceClass,
        String name,
        Class<? extends TBaseAsyncProcessor> processorClass,
        Class<?> clientFactoryClass) {
        this.ifaceClass = ifaceClass;
        this.name = name;
        this.processorClass = processorClass;
        this.clientFactoryClass = clientFactoryClass;
    }

    /**
     * @param handler service request handler
     * @return a new processor wrapping the given request handler
     */
    public TBaseAsyncProcessor buildProcessor(IFACE handler) {
        try {
            return processorClass.getConstructor(ifaceClass).newInstance(handler);
        } catch (Exception e) {
            throw new RuntimeException("Could not create processor with implementation: " + handler, e);
        }
    }

    /**
     * @param factory reusable protocol factory
     * @param manager reusable client manager
     * @return a new client factory for the service represented by this definition
     */
    public TAsyncClientFactory<? extends TAsyncClient> clientFactory(TProtocolFactory factory, TAsyncClientManager manager) {
        try {
            return (TAsyncClientFactory<? extends TAsyncClient>) clientFactoryClass.getConstructor(new Class<?>[]{TAsyncClientManager.class, TProtocolFactory.class}).newInstance(manager, factory);
        } catch (Exception e) {
            throw new RuntimeException("Could not create client factory", e);
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "{ThriftService:name=" + name + "}";
    }
}
