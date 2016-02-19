package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class RecordParquetWriterTest {
    private static final Logger logger = LogManager.getLogger(RecordParquetWriterTest.class);
    public static final int ROWS = 1024;

    @Test
    public void testWriting() throws Exception {
        ParquetLoggerOverride.fixParquetJUL();

        final String path = "target/test-file-" + System.currentTimeMillis() + ".par";

        final RecordParquetWriter writer = new RecordParquetWriter(
            new Path(path),
            newArrayList(new LongColumnHeader("Col1")));

        for (int i = 0; i < ROWS; i++) {
            final Record record = record(i);
            writer.write(record);
        }

        writer.close();

        final GenericParquetReader<MutableRecord> reader = new GenericParquetReader<>(new RecordReadSupport(), path, logger);
        for (int i = 0; i < ROWS; i++) {
            final MutableRecord retrieved = reader.read();
            assertEquals(retrieved.value(), (long) i);
        }
        assertNull(reader.read()); // EOF
        reader.close();
    }

    private Record record(int i) {
        return new Record((long) i);
    }
}
