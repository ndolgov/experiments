package net.ndolgov.thriftrpctest;

/**
 * Asynchronous Thrift service client factory
 */
public interface ClientFactory {
    /**
     * @param definition service definition to create client for
     * @param hostname server host name (IRL obtained from some discovery service)
     * @param <T> Thrift-generated service interface type
     * @return newly created service client
     */
    <T> T create(ServiceDefinition<T> definition, String hostname);

    void close();
}
