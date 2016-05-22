package net.ndolgov.s3test;

import com.amazonaws.services.s3.transfer.Transfer;

import java.io.File;

/**
 * Download a file from a given "subdirectory" of S3 bucket
 */
public final class S3Downloader implements FileDownloader {
    private final S3FileTransferClient client;

    public S3Downloader(S3FileTransferClient client) {
        this.client = client;
    }

    @Override
    public void download(File localFile, String remotePath, TransferCallback callback) {
        final S3TransferProgressListener listener = new S3TransferProgressListener(remotePath, localFile.getName(), callback);
        final Transfer download = client.download(remotePath, localFile, listener);
        listener.listenTo(download);
    }
}
