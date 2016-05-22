package net.ndolgov.s3test;

/**
 * Downloader-side handler of newly detected S3 storage files
 */
public interface FileHandler {
    /**
     * Handle a file found in the remote storage at a given path
     * @param remotePath S3 storage path
     * @param callback a means of reporting request status asynchronously
     */
    void handle(String remotePath, TransferCallback callback);
}
