package net.ndolgov.disruptortest;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Name and sequentially number created threads by service
 */
public final class DisruptorThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final ThreadGroup group = Thread.currentThread().getThreadGroup();
    private final String namePrefix;

    public DisruptorThreadFactory(String serviceName) {
        namePrefix = serviceName + "-thread-";
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement());
    }
}
