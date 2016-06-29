package net.ndolgov.disruptortest;

import com.lmax.disruptor.RingBuffer;

import java.util.function.Consumer;

/**
 * Copy fields from input buffer to the next vacant ring buffer slot
 * todo consider implementing com.lmax.disruptor.EventTranslator
 */
public final class DataRowEventTranslator implements Consumer<DataRow> {
    private final RingBuffer<DataRowEvent> ringBuffer;
    private int rowCount = 0;

    public DataRowEventTranslator(RingBuffer<DataRowEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void accept(DataRow inputBuffer) {
        final long seq = ringBuffer.next();
        ringBuffer.get(seq).from(inputBuffer);
        ringBuffer.publish(seq);

        rowCount++;
    }

    public int rowCount() {
        return rowCount;
    }
}
