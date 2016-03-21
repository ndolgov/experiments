package net.ndolgov.sqstest;

import java.util.concurrent.Future;

/**
 * Periodically reset visibility timeout for message still being processed
 */
public interface VisibilityTimeoutTracker {
    /**
     * @param handle message handle
     * @param timeout the next visibility timeout
     * @return the job to run after a given timeout
     */
    Future<?> track(String handle, int timeout);
}
