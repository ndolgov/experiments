package net.ndolgov.disruptortest;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Disruptor-based single producer/multiple consumer queue
 */
public final class ParallelProcessor {
    private final Executor producerExecutor;
    private final int nConsumers;
    private final int nRingSlots;

    public ParallelProcessor(Executor producerExecutor, int nConsumers, int nRingSlots) {
        this.producerExecutor = producerExecutor;
        this.nConsumers = nConsumers;
        this.nRingSlots = nRingSlots;
    }

    public ParallelProcessingContext process(Iterator<DataRow> input, Consumer<DataRow> output, long processingTimeout, long shutdownTimeout) {
        final Disruptor<DataRowEvent> disruptor = disruptor(nRingSlots);
        final DataRowEventConsumer[] consumers = consumers(nConsumers, output);
        disruptor.handleEventsWithWorkerPool(consumers);

        final RingBuffer<DataRowEvent> ringBuffer = disruptor.start();

        final DataRowEventProducer producer = new DataRowEventProducer(input, ringBuffer);
        producerExecutor.execute(producer);

        return new ParallelProcessingContext(disruptor, producer.isDoneLatch(), consumers, processingTimeout, shutdownTimeout);
    }

    private static DataRowEventConsumer[] consumers(int nConsumers, Consumer<DataRow> output) {
        final DataRowEventConsumer[] consumers = new DataRowEventConsumer[nConsumers];
        for (int i = 0; i < nConsumers; i++) {
            consumers[i] = new DataRowEventConsumer(output);
        }
        return consumers;
    }

    private static Disruptor<DataRowEvent> disruptor(int nRingSlots) {
        return new Disruptor<>(
            new DataRowEvent.DataRowEventFactory(),
            nRingSlots,
            new DisruptorThreadFactory("consumer"),
            ProducerType.SINGLE,
            new BlockingWaitStrategy());
    }
}
