package net.ndolgov.s3test;

import com.amazonaws.services.s3.transfer.Transfer;

import java.io.File;

/**
 * Upload a file to a subdirectory of S3 bucket
 */
public final class S3Uploader implements FileUploader {
    private final S3FileTransferClient client;

    public S3Uploader(S3FileTransferClient client) {
        this.client = client;
    }

    @Override
    public void upload(File localFile, String remotePath, TransferCallback callback) {
        final S3TransferProgressListener listener = new S3TransferProgressListener(localFile.getName(), remotePath, callback);
        final Transfer upload = client.upload(localFile, remotePath, listener);
        listener.listenTo(upload);
    }
}
