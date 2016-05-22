package net.ndolgov.s3test;

/**
 * S3 client JMX end point
 */
public interface S3ClientMBean {
    /**
     * @return S3 bucket
     */
    String getBucket();

    /**
     * @return a prefix path automatically inserted between the bucket name and a relative path to a file
     */
    String getPathPrefix();
}
