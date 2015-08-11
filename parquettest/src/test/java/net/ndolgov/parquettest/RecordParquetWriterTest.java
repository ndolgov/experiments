package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;

public class RecordParquetWriterTest {
    public static final int ROWS = 1024;

    @Test
    public void testWriting() throws Exception {
        ParquetLoggerOverride.fixParquetJUL();

        final RecordParquetWriter writer = new RecordParquetWriter(
            new Path("target/test-file-" + System.currentTimeMillis() + ".par"),
            newArrayList(new ColumnHeader() {
                @Override
                public String name() {
                    return "Col1";
                }

                @Override
                public ColumnType type() {
                    return ColumnType.LONG;
                }
            }));

        for (int i = 0; i < ROWS; i++) {
            final Record record = record(i);
            writer.write(record);
        }

        writer.close();
    }

    private Record record(int i) {
        return new Record((long) i);
    }
}
