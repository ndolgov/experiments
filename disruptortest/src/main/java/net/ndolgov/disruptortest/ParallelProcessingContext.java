package net.ndolgov.disruptortest;

import com.lmax.disruptor.dsl.Disruptor;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Allow to await the end of processing and clean up allocated resources afterwards. A latch allows to wait for
 * the producer to completely process input. A shutdown timeout allows consumers finish processing of remaining items.
 */
public final class ParallelProcessingContext {
    private final Disruptor<DataRowEvent> disruptor;

    private final DataRowEventConsumer[] consumers;

    private final CountDownLatch producerIsDone;

    private final long processingTimeout; // is not expected to be reached unless an unexpected error happens

    private final long shutdownTimeout; // allows consumers to finish processing enqueued events after processor is finished

    public ParallelProcessingContext(Disruptor<DataRowEvent> disruptor,
                                     CountDownLatch producerIsDone,
                                     DataRowEventConsumer[] consumers,
                                     long processingTimeout,
                                     long shutdownTimeout) {
        this.consumers = consumers;
        this.processingTimeout = processingTimeout;
        this.shutdownTimeout = shutdownTimeout;
        this.producerIsDone = producerIsDone;
        this.disruptor = disruptor;
    }

    public boolean await() {
        try {
            return producerIsDone.await(processingTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Processing took too long", e);
        }
    }

    public void shutDown() {
        try {
            disruptor.shutdown(shutdownTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Consumers took too long to shutdown", e);
        }
    }

    public int totalRowCount() {
        return Arrays.stream(consumers).map(DataRowEventConsumer::rowCount).reduce(0, (a, b) -> a + b);
    }
}
