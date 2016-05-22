package net.ndolgov.s3test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Check pre-configured S3 location for new files. If found, trigger their download and handling.
 */
public final class S3ChangeDetector implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(S3ChangeDetector.class);

    private final FileHandler handler;

    private final S3FileTransferClient client;

    private final Set<String> knownObjects; // todo in real life, have a more realistic means such as versions

    private final AtomicLong lastScanTime = new AtomicLong();

    public S3ChangeDetector(FileHandler handler, S3FileTransferClient client) {
        this.handler = handler;
        this.client = client;
        this.knownObjects = new HashSet<>();
    }

    public void run() {
        lastScanTime.set(System.currentTimeMillis());
        findAndHandlePreviouslyUnseenFiles();
    }

    private void findAndHandlePreviouslyUnseenFiles() {
        client.list(remotePath -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Checking: " + remotePath);
            }

            try {
                if (!knownObjects.contains(remotePath)) {
                    handle(remotePath);
                    knownObjects.add(remotePath);
                }
            } catch (Exception e) {
                logger.warn("Failed to process S3 key: " + remotePath + " because of: " + e.getMessage());
            }

            return null;
        });
    }

    private void handle(String remotePath) {
        logger.info("Processing new file notification: " + remotePath);

        try {
            handler.handle(remotePath, new TransferCallback() {
                public void onSuccess() {
                    logger.info("Downloaded from: " + remotePath);
                }

                public void onFailure(String message) {
                    logger.error("Could not download from: " + remotePath);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to process new file notification: " + remotePath, e);
        }
    }
}
