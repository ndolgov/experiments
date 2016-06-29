package net.ndolgov.disruptortest;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * Pour data rows from input iterator into ring buffer
 */
public final class DataRowEventProducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DataRowEventProducer.class);

    private final Iterator<DataRow> input;

    private final DataRowEventTranslator translator;

    private final CountDownLatch producerIsDone;

    public DataRowEventProducer(Iterator<DataRow> input, RingBuffer<DataRowEvent> ringBuffer) {
        this.input = input;
        this.translator = new DataRowEventTranslator(ringBuffer);
        this.producerIsDone = new CountDownLatch(1);
    }

    @Override
    public void run() {
        input.forEachRemaining(translator);

        logger.info("Producer finished after processing rows: " + translator.rowCount());

        producerIsDone.countDown();
    }

    public CountDownLatch isDoneLatch() {
        return producerIsDone;
    }
}
