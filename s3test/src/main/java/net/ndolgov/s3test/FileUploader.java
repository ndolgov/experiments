package net.ndolgov.s3test;

import java.io.File;

/**
 * Upload a file to S3 storage
 */
public interface FileUploader {
    /**
     * @param localFile local file to upload
     * @param remotePath relative path (counting from some assumed root) in S3 storage
     * @param callback observer to notify about upload status
     */
    void upload(File localFile, String remotePath, TransferCallback callback);
}
