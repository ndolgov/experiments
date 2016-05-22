package net.ndolgov.s3test;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * S3 progress event handler. It recognizes only two final transfer states and so treats cancellations as failures.
 */
public final class S3TransferProgressListener implements ProgressListener {
    private static final Logger logger = LoggerFactory.getLogger(S3TransferProgressListener.class);
    private static final int MAX_TRANSFER_TIMEOUT = 10;
    private final String debugMsg;
    private final TransferCallback callback;
    private final CompletableFuture<Transfer> future = new CompletableFuture<>();

    public S3TransferProgressListener(String src, String dest, TransferCallback callback) {
        this.callback = callback;
        this.debugMsg = src + " -> " + dest;
    }

    /**
     * Set transfer request object used to determine transfer error
     * @param request transfer request handler
     */
    public void listenTo(Transfer request) {
        future.complete(request);
    }

    @Override
    public void progressChanged(ProgressEvent event) {
        switch (event.getEventType()) {
            case TRANSFER_COMPLETED_EVENT:
                logger.info("Completed: " + debugMsg);
                callback.onSuccess();
                break;

            case TRANSFER_FAILED_EVENT:
                logger.info("Failed: " + debugMsg);
                callback.onFailure(errorMessage());
                break;

            case TRANSFER_CANCELED_EVENT:
                logger.info("Cancelled: " + debugMsg);
                callback.onFailure(errorMessage());
                break;
        }
    }

    private String errorMessage() {
        try {
            return future.get(MAX_TRANSFER_TIMEOUT, TimeUnit.SECONDS).waitForException().getMessage();
        } catch (InterruptedException e) {
            return "Interrupted while waiting for upload";
        } catch (ExecutionException e) {
            return e.getMessage();
        } catch (TimeoutException e) {
            return "Time out reached while waiting for an error message";
        }
    }
}
