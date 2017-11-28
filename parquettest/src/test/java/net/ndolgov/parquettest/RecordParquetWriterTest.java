package net.ndolgov.parquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static net.ndolgov.parquettest.RecordFields.METRIC;
import static net.ndolgov.parquettest.RecordFields.ROW_ID;
import static net.ndolgov.parquettest.RecordFields.TIME;
import static net.ndolgov.parquettest.RecordFields.VALUE;
import static net.ndolgov.parquettest.RecordFileUtil.createParquetFile;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class RecordParquetWriterTest {
    private static final String PATH = "target/test-file-" + System.currentTimeMillis() + ".par";
    private static final int ROWS = 1024;
    private static final String SCHEMA_VERSION = "SCHEMA_VERSION";
    private static final String V3 = "3";
    

    @Test
    public void testWritingAndReading() throws Exception {
        ParquetLoggerOverride.fixParquetJUL();

        final Map<String, String> metadata = newHashMap();
        metadata.put(SCHEMA_VERSION, V3);

        createRecordFile(PATH, metadata);

        assertCustomMetadata(metadata(PATH));
        assertMetadata(PATH);

        final GenericParquetReader<Record> reader = new GenericParquetReader<>(new RecordReadSupport(), PATH);
        for (int i = 0; i < ROWS; i++) {
            final Record retrieved = reader.read();
            assertEquals(retrieved.getLong(ROW_ID.index()), (long) i);
            assertEquals(retrieved.getLong(METRIC.index()), (long) i);
            assertEquals(retrieved.getLong(TIME.index()), (long) i);
            assertEquals(retrieved.getLong(VALUE.index()), (long) i);
        }
        assertNull(reader.read()); // EOF
        reader.close();

        assertDisjunctiveFilters(PATH);
        assertUserDefinedFilter(PATH);
    }

    private void createRecordFile(String path, Map<String, String> metadata) throws IOException {
        final List<Record> rows = newArrayList();
        for (int i = 0; i < ROWS; i++) {
            rows.add(record(i));
        }

        createParquetFile(path, rows, metadata);
    }

    private static void assertDisjunctiveFilters(String path) {
        final long minValue = 0L;
        final long maxValue = ROWS - 1;

        final FilterCompat.Filter filter = FilterCompat.get(
            or(
                eq(
                    longColumn(ROW_ID.columnName()),
                    minValue),
                eq(
                    longColumn(ROW_ID.columnName()),
                    maxValue)
            ));

        final GenericParquetReader<Record> filtered = new GenericParquetReader<>(new RecordReadSupport(), path, filter);
        assertEquals(filtered.read().getLong(ROW_ID.index()), minValue);
        assertEquals(filtered.read().getLong(ROW_ID.index()), maxValue);
        assertNull(filtered.read()); // EOF
        filtered.close();
    }

    private static void assertUserDefinedFilter(String path) {
        final long value = ROWS / 2;

        final FilterCompat.Filter filter = FilterCompat.get(
            userDefined(
                longColumn(ROW_ID.columnName()),
                new FilterByValue(value)));

        final GenericParquetReader<Record> filtered = new GenericParquetReader<>(new RecordReadSupport(), path, filter);
        assertEquals(filtered.read().getLong(ROW_ID.index()), value);
        assertNull(filtered.read()); // EOF
        filtered.close();
    }

    /**
     * See https://github.com/Parquet/parquet-format/raw/master/doc/images/FileFormat.gif for a metadata class diagram
     */
    private static void assertMetadata(String path) throws Exception {
        final ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), new Path(path), NO_FILTER);

        final FileMetaData fileMetaData = footer.getFileMetaData();
        assertEquals(fileMetaData.getSchema().getName(), ToParquet.SCHEMA_NAME);
        assertCustomMetadata(fileMetaData.getKeyValueMetaData());

        final List<BlockMetaData> blocks = footer.getBlocks();
        assertEquals(blocks.size(), 1);

        final BlockMetaData block = blocks.get(0);
        assertEquals(block.getRowCount(), ROWS);
        assertEquals(block.getColumns().size(), 4);

        final ColumnChunkMetaData column = block.getColumns().get(0);
        assertEquals(column.getType(), PrimitiveType.PrimitiveTypeName.INT64);

        final Statistics stats = column.getStatistics();
        assertEquals(stats.getNumNulls(), 0);
        assertTrue(stats.hasNonNullValue());
        assertEquals(BytesUtils.bytesToLong(stats.getMinBytes()), 0);
        assertEquals(BytesUtils.bytesToLong(stats.getMaxBytes()), ROWS - 1);
    }

    private static void assertCustomMetadata(Map<String, String> metadata) {
        assertEquals(metadata.size(), 2);
        assertEquals(metadata.get(SCHEMA_VERSION), V3);
        assertEquals(metadata.get(RecordWriteSupport.ROW_COUNT), String.valueOf(ROWS));
    }

    private static Record record(long i) {
        return new Record(i, i, i, i);
    }

    private static Map<String, String> metadata(String path) {
        final RecordReadSupport support = new RecordReadSupport();

        final GenericParquetReader<Record> file = new GenericParquetReader<>(support, path);
        file.read(); // trigger read support initialization
        final Map<String, String> metadata = support.metadata();
        file.close();

        return metadata;
    }
}
