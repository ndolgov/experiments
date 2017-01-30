package net.ndolgov.timeseriescompression;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public final class CompressedTimeSeriesTest {
    @Test
    public void testEmpty() {
        read(new byte[0], 0, new long[0], new double[0]);
    }

    @Test
    public void testMultipleDataPoints() {
        final int dpCount = 6;
        final byte[] buffer = new byte[16 * dpCount];

        final CompressedTimeSeries timeSeries = new CompressedTimeSeries(buffer);
        final long[] originalTimestamps = {0, 60, 120, 180, 240, 300};
        final double[] originalValues = {5.0, 6.0, 7.0, 7.0, 8.0, 0.3333};

        for (int i = 0; i < originalValues.length; i++) {
            append(timeSeries, originalTimestamps[i], originalValues[i]);
        }

        final long[] retrievedTimes = new long[dpCount];
        final double[] retrievedValues = new double[dpCount];
        read(buffer, dpCount, retrievedTimes, retrievedValues);

        for (int i = 0; i < dpCount; i++) {
            assertEquals(originalTimestamps[i], retrievedTimes[i]);
            assertEquals(originalValues[i], retrievedValues[i]);
        }
    }

    private static void append(CompressedTimeSeries stream, long time, double value) {
        if (!stream.append(time, value)) {
            throw new IllegalArgumentException("timestamp:" + time);
        }
    }

    private static void read(byte[] data, int count, long[] times, double[] values) {
        if (count == 0) {
            return;
        }

        final CompressedTimeSeries timeSeries = new CompressedTimeSeries(data);

        times[0] = timeSeries.readFirstTimeStamp();
        values[0] = timeSeries.readNextValue();
        int readSoFar = 1;

        while (readSoFar < count) {
            times[readSoFar] = timeSeries.readNextTimestamp();
            values[readSoFar] = timeSeries.readNextValue();
            readSoFar++;
        }
    }
}
