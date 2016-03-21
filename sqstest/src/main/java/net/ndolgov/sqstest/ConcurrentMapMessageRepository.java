package net.ndolgov.sqstest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ConcurrentMapMessageRepository implements MessageRepository {
    private final ConcurrentMap<String, String> handleToQueue;

    public ConcurrentMapMessageRepository() {
        this.handleToQueue = new ConcurrentHashMap<>(64);
    }

    @Override
    public String get(String handle) {
        return handleToQueue.get(handle);
    }

    @Override
    public String remove(String handle) {
        return handleToQueue.remove(handle);
    }

    @Override
    public String put(String handle, String queueUrl) {
        handleToQueue.put(handle, queueUrl);
        return queueUrl;
    }
}
