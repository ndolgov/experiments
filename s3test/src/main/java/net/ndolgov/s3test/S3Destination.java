package net.ndolgov.s3test;

/**
 * A means of having a common S3 parent path for multiple files
 */
public interface S3Destination {
    /** Our S3 path delimiter */
    String DELIMITER = "/";

    /**
     * @param namespace AWS namespace
     * @return namespace-based prefix to restrict S3 keys for this destination
     */
    String prefix(String namespace);
}
