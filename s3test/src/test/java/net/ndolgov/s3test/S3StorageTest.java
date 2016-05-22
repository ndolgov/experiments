package net.ndolgov.s3test;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.easymock.Capture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.google.common.collect.Lists.newArrayList;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public final class S3StorageTest {
    private static final Logger logger = LoggerFactory.getLogger(S3StorageTest.class);
    private static final String REMOTE_PATH = "relative/path/test.txt";

    @Test
    public void testUploadAndChangeDetectionCycle() throws Exception {
        final long now = System.currentTimeMillis();
        final File fileToUpload = new File("target/test" + now + ".txt");
        fileToUpload.createNewFile();
        final File downloadFileTo = File.createTempFile("pre" + now, "post" + now);

        final S3Client s3Client = new S3Client(awsS3Client(), "BUCKET", "NAMESPACE", namespace -> namespace + "/v1");
        final FileDownloader downloader = new S3Downloader(s3Client);
        final FileUploader uploader = new S3Uploader(s3Client);

        runtTest(fileToUpload, downloadFileTo, s3Client, downloader, uploader);
    }

    private void runtTest(File original, File downloaded, S3Client s3Client, FileDownloader downloader, FileUploader uploader) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        uploader.upload(original, REMOTE_PATH, new TransferCallback() {
            @Override
            public void onSuccess() {
                logger.info("Uploaded " + original.getAbsolutePath());
            }

            @Override
            public void onFailure(String message) {
                logger.error("Could not upload " + original.getAbsolutePath());
            }
        });

        final S3ChangeDetector detector = new S3ChangeDetector(
            new FileHandler() {
                @Override
                public void handle(String remotePath, TransferCallback callback) {
                    downloader.download(downloaded, remotePath, new TransferCallback() {
                        @Override
                        public void onSuccess() {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(String message) {
                            logger.error("Could not download " + original.getAbsolutePath());
                        }
                    });
                }
            },
            s3Client);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(detector, 1, 30_000, TimeUnit.MILLISECONDS);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            Assert.fail("S3 synchronization failed");
        }
    }

    private static TransferManager awsS3Client() {
        final TransferManager manager = createMock(TransferManager.class);

        final Capture<PutObjectRequest> putRequest = new Capture<>();
        expect(manager.upload(capture(putRequest))).andAnswer(() -> {
            logger.info("Simulating successful file upload to: "  + putRequest.getValue().getKey());
            putRequest.getValue().getGeneralProgressListener().progressChanged(new ProgressEvent(TRANSFER_COMPLETED_EVENT));
            return createMock(Upload.class);
        });

        final Capture<GetObjectRequest> getRequest = new Capture<>();
        final Capture<File> file = new Capture<>();
        expect(manager.download(capture(getRequest), capture(file))).andAnswer(() -> {
            logger.info("Simulating successful file download to: "  + file.getValue().getAbsolutePath());
            putRequest.getValue().getFile().renameTo(file.getValue());
            getRequest.getValue().getGeneralProgressListener().progressChanged(new ProgressEvent(TRANSFER_COMPLETED_EVENT));
            return createMock(Download.class);
        });

        final AmazonS3 aws = createMock(AmazonS3.class);
        expect(aws.listObjects(anyObject(ListObjectsRequest.class))).andReturn(objectListing(REMOTE_PATH));
        replay(aws);

        expect(manager.getAmazonS3Client()).andReturn(aws);
        replay(manager);

        return manager;
    }

    private static ObjectListing objectListing(String key) {
        return new ObjectListing() {
            @Override
            public List<S3ObjectSummary> getObjectSummaries() {
                final S3ObjectSummary summary = new S3ObjectSummary();
                summary.setKey(key);
                return newArrayList(summary);
            }
        };
    }
}
