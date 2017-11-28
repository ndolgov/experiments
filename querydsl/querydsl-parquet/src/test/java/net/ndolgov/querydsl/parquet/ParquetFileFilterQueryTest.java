package net.ndolgov.querydsl.parquet;

import net.ndolgov.parquettest.GenericParquetReader;
import net.ndolgov.parquettest.Record;
import net.ndolgov.parquettest.RecordReadSupport;
import net.ndolgov.querydsl.antlr.action.AntlrActionDslParser;
import net.ndolgov.querydsl.antlr.listener.AntlrListenerDslParser;
import net.ndolgov.querydsl.fastparse.FastparseDslParser;
import net.ndolgov.querydsl.parboiled.ParboiledDslParser;
import net.ndolgov.querydsl.parser.DslParser;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static net.ndolgov.parquettest.RecordFields.ROW_ID;
import static net.ndolgov.parquettest.RecordFileUtil.createParquetFile;
import static org.testng.Assert.assertEquals;

public final class ParquetFileFilterQueryTest {
    private static final long LONG_VALUE1 = 123L;
    private static final long LONG_VALUE2 = 456L;
    private static final long LONG_VALUE3 = 789L;

    private static final long METRIC1 = 123456L;
    private static final long METRIC2 = 456789L;
    private static final long METRIC3 = 234567L;

    @Test
    public void testColumnEquality() throws IOException {
        final String path = createRecordFile("target/pffq-" + System.currentTimeMillis() + ".par");

        assertColumnEquality(path, new ParboiledDslParser());

        assertColumnEquality(path, new AntlrListenerDslParser());

        assertColumnEquality(path, new AntlrActionDslParser());

        assertColumnEquality(path, FastparseDslParser.apply());
    }

    private static void assertColumnEquality(String path, DslParser parser) {
        final String disjunctiveQuery = String.format(
            "select %d,%d from '%s' where ('TIME'=%d) || ('TIME'=%d)",
            METRIC1, METRIC2,
            path,
            LONG_VALUE1, LONG_VALUE2
            );
        assertRowIds(newArrayList(11L, 13L, 14L, 21L, 23L, 24L), path, disjunctiveQuery, parser);

        final String conjunctiveQuery = String.format(
            "select %d from '%s' where ('TIME'=%d) && ('VALUE'=%d)",
            METRIC1,
            path,
            LONG_VALUE1, LONG_VALUE2
        );
        assertRowIds(newArrayList(13L), path, conjunctiveQuery, parser);
    }

    private static void assertRowIds(List<Long> rowIds, String filePath, String query, DslParser parser) {
        final FilterCompat.Filter filter = ParquetQueryBuilder.build(parser.parse(query));
        assertEquals(rowIds(fileReader(filePath, filter)), rowIds);
    }

    private static GenericParquetReader<Record> fileReader(String path, FilterCompat.Filter filters) {
        return new GenericParquetReader<>(new RecordReadSupport(), path, filters);
    }
    
    private static List<Long> rowIds(GenericParquetReader<Record> reader) {
        final List<Long> rowIds = new ArrayList<>();

        try {
            Record row = reader.read();
            while (row != null) {
                //logger.info("Row: " + row);
                rowIds.add(row.getLong(ROW_ID.index()));
                row = reader.read();
            }
        } finally {
            reader.close();
        }

        return rowIds;
    }

    private String createRecordFile(String path) throws IOException {
        final List<Record> rows = newArrayList();

        rows.add(new Record(10, METRIC1, LONG_VALUE3, LONG_VALUE3));
        rows.add(new Record(11, METRIC1, LONG_VALUE1, LONG_VALUE3));
        rows.add(new Record(12, METRIC1, LONG_VALUE3, LONG_VALUE1));
        rows.add(new Record(13, METRIC1, LONG_VALUE1, LONG_VALUE2));
        rows.add(new Record(14, METRIC1, LONG_VALUE1, LONG_VALUE1));
        rows.add(new Record(20, METRIC2, LONG_VALUE3, LONG_VALUE3));
        rows.add(new Record(21, METRIC1, LONG_VALUE2, LONG_VALUE3));
        rows.add(new Record(22, METRIC1, LONG_VALUE3, LONG_VALUE2));
        rows.add(new Record(23, METRIC1, LONG_VALUE2, LONG_VALUE1));
        rows.add(new Record(24, METRIC1, LONG_VALUE2, LONG_VALUE2));
        rows.add(new Record(30, METRIC3, LONG_VALUE3, LONG_VALUE3));

        createParquetFile(path, rows, new HashMap<>());
        return path;
    }

}
