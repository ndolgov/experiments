package net.ndolgov.flatbufferstest;

import com.google.flatbuffers.FlatBufferBuilder;
import net.ndolgov.fbstest.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.testng.Assert.assertEquals;

public final class FlatBufferTest {
    private static final int OVERHEAD = 56;
    private static Logger logger = LoggerFactory.getLogger(FlatBufferTest.class);

    private static final int DATA_POINT_COUNT = 10;

    @Test
    public void testSerializationCycle() {
        final long now = System.currentTimeMillis();

        final long[] times = new long[DATA_POINT_COUNT];
        final double[] values = new double[DATA_POINT_COUNT];
        for (int i = 0; i < DATA_POINT_COUNT; i++) {
            times[i] = now + i*10000;
            values[i] = i * 123.456;
        }

        deserialize(times, values, serialize(times, values, createReusableBuffer(DATA_POINT_COUNT * 16 + 4 + OVERHEAD)));
    }

    private ByteBuffer createReusableBuffer(int size) {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        logger.info("Initial : " + System.identityHashCode(buffer) + " " + buffer);

        return buffer;
    }

    private static void deserialize(long[] times, double[] values, ByteBuffer buffer) {
        final TimeSeries timeSeries = TimeSeries.getRootAsTimeSeries(buffer);

        assertEquals(timeSeries.dataPointCount(), DATA_POINT_COUNT);
        assertEquals(timeSeries.timesLength(), DATA_POINT_COUNT);
        assertEquals(timeSeries.valuesLength(), DATA_POINT_COUNT);

        for (int i = 0; i < DATA_POINT_COUNT; i++) {
            assertEquals(timeSeries.times(i), times[i]);
            assertEquals(timeSeries.values(i), values[i]);
        }
    }

    private static ByteBuffer serialize(long[] times, double[] values, ByteBuffer initialBuffer) {
        final FlatBufferBuilder builder = new FlatBufferBuilder(initialBuffer);

        final int timeOffset = TimeSeries.createTimesVector(builder, times);
        final int valueOffset = TimeSeries.createValuesVector(builder, values);

        TimeSeries.startTimeSeries(builder);
        TimeSeries.addDataPointCount(builder, DATA_POINT_COUNT);
        TimeSeries.addTimes(builder, timeOffset);
        TimeSeries.addValues(builder, valueOffset);
        final int offset = TimeSeries.endTimeSeries(builder);

        builder.finish(offset);

        final ByteBuffer buffer = builder.dataBuffer();

        logger.info("Times@ : " + timeOffset);
        logger.info("Values@: " + valueOffset);
        logger.info("Offset : " + offset);
        logger.info("Final  : " + System.identityHashCode(buffer) + " " + buffer);

        return buffer;
    }
}
