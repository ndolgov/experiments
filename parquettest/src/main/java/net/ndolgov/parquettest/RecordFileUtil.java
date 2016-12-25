package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Parquet file operations
 */
public final class RecordFileUtil {
    public static void createParquetFile(String path, List<Record> rows, Map<String, String> metadata) throws IOException {
        final RecordParquetWriter writer = new RecordParquetWriter(
            new Path(path),
            newArrayList(
                new LongColumnHeader(RecordFields.ROW_ID.columnName()),
                new LongColumnHeader(RecordFields.METRIC.columnName()),
                new LongColumnHeader(RecordFields.TIME.columnName()),
                new LongColumnHeader(RecordFields.VALUE.columnName())),
            metadata);

        try {
            rows.forEach(row -> {
                try {
                    writer.write(row);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            });
        } finally {
            writer.close();
        }

    }


}
