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
    private static final String V2 = "2";
    private static final String COLUMN_NAME = "Col1";

    @Test
    public void testWritingAndReading() throws Exception {
        ParquetLoggerOverride.fixParquetJUL();

        final Map<String, String> metadata = newHashMap();
        metadata.put(SCHEMA_VERSION, V2);

        createParquetFile(PATH, metadata);

        assertCustomMetadata(metadata(PATH));
        assertMetadata(PATH);

        final GenericParquetReader<MutableRecord> reader = new GenericParquetReader<>(new RecordReadSupport(), PATH);
        for (int i = 0; i < ROWS; i++) {
            final MutableRecord retrieved = reader.read();
            assertEquals(retrieved.value(), (long) i);
        }
        assertNull(reader.read()); // EOF
        reader.close();

        assertDisjunctiveFilters(PATH);
        assertUserDefinedFilter(PATH);
    }

    private static void assertDisjunctiveFilters(String path) {
        final long minValue = 0L;
        final long maxValue = ROWS - 1;

        final FilterCompat.Filter filter = FilterCompat.get(
            or(
                eq(
                    longColumn(COLUMN_NAME),
                    minValue),
                eq(
                    longColumn(COLUMN_NAME),
                    maxValue)
            ));

        final GenericParquetReader<MutableRecord> filtered = new GenericParquetReader<>(new RecordReadSupport(), path, filter);
        assertEquals(filtered.read().value(), minValue);
        assertEquals(filtered.read().value(), maxValue);
        assertNull(filtered.read()); // EOF
        filtered.close();
    }

    private static void assertUserDefinedFilter(String path) {
        final long value = ROWS / 2;

        final FilterCompat.Filter filter = FilterCompat.get(
            userDefined(
                longColumn(COLUMN_NAME),
                new FilterByValue(value)));

        final GenericParquetReader<MutableRecord> filtered = new GenericParquetReader<>(new RecordReadSupport(), path, filter);
        assertEquals(filtered.read().value(), value);
        assertNull(filtered.read()); // EOF
        filtered.close();
    }

    private static void createParquetFile(String path, Map<String, String> metadata) throws IOException {
        final RecordParquetWriter writer = new RecordParquetWriter(
            new Path(path),
            newArrayList(new LongColumnHeader(COLUMN_NAME)),
            metadata);

        for (int i = 0; i < ROWS; i++) {
            final Record record = record(i);
            writer.write(record);
        }

        writer.close();
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
        assertEquals(block.getColumns().size(), 1);

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
        assertEquals(metadata.get(SCHEMA_VERSION), V2);
        assertEquals(metadata.get(RecordWriteSupport.ROW_COUNT), String.valueOf(ROWS));
    }

    private static Record record(int i) {
        return new Record((long) i);
    }

    private static Map<String, String> metadata(String path) {
        final RecordReadSupport support = new RecordReadSupport();

        final GenericParquetReader<MutableRecord> file = new GenericParquetReader<>(support, path);
        file.read(); // trigger read support initialization
        final Map<String, String> metadata = support.metadata();
        file.close();

        return metadata;
    }
}
