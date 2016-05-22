package net.ndolgov.s3test;

import java.io.File;

/**
 * Download a file from S3 storage
 */
public interface FileDownloader {
    /**
     * @param localFile local file to upload
     * @param remotePath relative path (counting from some assumed root) in S3 storage
     * @param callback observer to notify about upload status
     */
    void download(File localFile, String remotePath, TransferCallback callback);
}
