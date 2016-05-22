package net.ndolgov.s3test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.function.Function;

/**
 * Use <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManager.html">transfer manager</a>
 * to upload and download file asynchronously.<p>
 *
 * The expected S3 key naming convention is: "/$NAMESPACE/$PREFIX/$key".
 */
public final class S3Client implements S3FileTransferClient, S3ClientMBean {
    private static final Logger logger = LoggerFactory.getLogger(S3Client.class);

    private final String bucket;

    /** namespace and service prefix {@link #prepend prepended} to relative paths */
    private final String prefix;

    private final TransferManager manager;

    public S3Client(AmazonS3Client client, String bucket, String namespace, S3Destination destination) {
        this(new TransferManager(client), bucket, namespace, destination);
    }

    public S3Client(TransferManager manager, String bucket, String namespace, S3Destination destination) {
        this.bucket = bucket;
        this.prefix = destination.prefix(namespace);
        this.manager = manager;
    }

    @Override
    public Upload upload(File localFile, String key, ProgressListener listener) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(localFile.length());

        return manager.upload(new PutObjectRequest(bucket, prepend(key), localFile).
            withMetadata(metadata).
            withGeneralProgressListener(listener));
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getPathPrefix() {
        return prefix;
    }

    /**
     * @param relativePath application-specific file path (e.g. "some_path/file_name.ext")
     * @return "$PREFIX/$relativePath"-compliant version of relativePath
     */
    private String prepend(String relativePath) {
        return prefix + relativePath;
    }

    @Override
    public Download download(String key, File localFile, ProgressListener listener) {
        return manager.download(new GetObjectRequest(bucket, prepend(key)).withGeneralProgressListener(listener), localFile);
    }

    @Override
    public void list(Function<String, Void> consumer) {
        final ListObjectsRequest request = new ListObjectsRequest().
            withBucketName(bucket).
            withPrefix(this.prefix);

        listObjects(consumer, request);
    }

    @Override
    public void listOneLevel(Function<String, Void> consumer, String subDir) {
        final String prefix = this.prefix + (subDir == null ? "" : subDir);

        final ListObjectsRequest request = new ListObjectsRequest().
            withBucketName(bucket).
            withPrefix(prefix).
            withDelimiter(S3Destination.DELIMITER);

        listObjects(consumer, request);
    }

    private void listObjects(Function<String, Void> consumer, ListObjectsRequest request) {
        ObjectListing objects;
        do {
            objects = manager.getAmazonS3Client().listObjects(request);
            for (S3ObjectSummary object : objects.getObjectSummaries()) {
                consumer.apply(object.getKey());
            }
            request.setMarker(objects.getNextMarker());
        } while (objects.isTruncated());
    }

    public void close() {
        manager.shutdownNow();
    }

    @Override
    public boolean exists(String relativePath) {
        try {
            final ObjectMetadata metadata = manager.getAmazonS3Client().getObjectMetadata(bucket, prepend(relativePath));
            return true;
        } catch (AmazonServiceException e) {
            return false;
        }
    }

    @Override
    public boolean delete(String objectKey) {
        try {
            manager.getAmazonS3Client().deleteObject(bucket, objectKey);
            return true;
        } catch (AmazonClientException e) {
            logger.warn("Failed to delete object: " + objectKey, e);
            return false;
        }
    }

    @Override
    public String toString() {
        return "{S3Client:bucket=" + bucket + ", prefix=" + prefix + "}";
    }
}
