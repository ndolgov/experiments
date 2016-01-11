package net.ndolgov.thriftrpctest;

/**
 * Asynchronous Thrift service server-side handler factory
 */
public interface HandlerFactory {
    /**
     * @param definition service definition to create server-side handler for
     * @param <T> Thrift-generated service interface type
     * @return newly created service request handler
     */
    <T> T handler(ServiceDefinition<T> definition);
}
