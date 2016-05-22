package net.ndolgov.s3test;

/**
 * A means of tracking asynchronous file transfer completion
 */
public interface TransferCallback {
    /**
     * Notify about successful file transfer
     */
    void onSuccess();

    /**
     * Notify about file transfer failure
     * @param message error message
     */
    void onFailure(String message);

    /**
     * For cases when nobody cares about request status
     */
    TransferCallback IGNORED = new TransferCallback() {
        public void onSuccess() {
        }

        public void onFailure(String message) {
        }
    };
}
