package net.ndolgov.s3test;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.util.function.Function;

/**
 * Asynchronous transfer operations on files in one S3 bucket
 */
public interface S3FileTransferClient {
    /**
     *
     * @param src absolute source file path
     * @param key a path in the bucket
     * @param listener continuation to run when a final transfer state is reached
     * @return Upload request handle
     */
    Upload upload(File src, String key, ProgressListener listener);

    /**
     *
     * @param key a path in the bucket
     * @param dest local file to download to
     * @param listener continuation to run when a final transfer state is reached
     * @return Download request handle
     */
    Download download(String key, File dest, ProgressListener listener);

    /**
     * List all object located under the root
     * @param consumer consumer of found object keys
     */
    void list(Function<String, Void> consumer);

    /**
     * List all object located directly under the root or a given subdirectory
     * @param consumer consumer of found object keys
     * @param subDir subdirectory under root (e.g. "some_path") or <tt>null</tt> to search under root
     */
    void listOneLevel(Function<String, Void> consumer, String subDir);

    /**
     * @param key a path in the bucket
     * @return <tt>true</tt> if there is an object with the given key
     */
    boolean exists(String key);

    /**
     * @param objectKey object key (i.e. <b>absolute</b> path in the bucket)
     * @return <tt>true</tt> if there the object with the given key was deleted
     */
    boolean delete(String objectKey);
}
