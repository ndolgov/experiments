package net.ndolgov.parquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Implementation of {@link ReadSupport} for reading PrimitiveTypeRows of ColumnHeader-based datasets.
 */
public final class RecordReadSupport extends ReadSupport<Record> {
    private final Map<String, String> metadata = new HashMap<>(32);

    @Override
    public ReadContext init(InitContext context) {
        metadata.putAll(context.getMergedKeyValueMetaData());
        return new ReadContext(context.getFileSchema(), context.getMergedKeyValueMetaData());
    }

    @Override
    public RecordMaterializer<Record> prepareForRead(Configuration configuration,
                                                Map<String, String> keyValueMetaData,
                                                MessageType fileSchema,
                                                ReadContext readContext) {
        return new MessageReader(inferByType(fileSchema));
    }

    /**
     * @return custom metadata associated with this file
     */
    public Map<String, String> metadata() {
        return metadata;
    }

    private static final class MessageReader extends RecordMaterializer<Record> {
        private final PrimitiveTypeRowGroupConverter root;
        private final Record record;

        public MessageReader(List<ColumnHeader> headers) {
            record = new Record(Record.NULL, Record.NULL, Record.NULL, Record.NULL);
            root = new PrimitiveTypeRowGroupConverter(fieldReaders(headers, record));
        }

        @Override
        public Record getCurrentRecord() {
            return record;
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }
    }

    private static final class PrimitiveTypeRowGroupConverter extends GroupConverter {
        private final FieldReader[] fieldReaders;

        private PrimitiveTypeRowGroupConverter(FieldReader[] fieldReaders) {
            this.fieldReaders = fieldReaders;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return fieldReaders[fieldIndex];
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    }

    private static abstract class FieldReader extends PrimitiveConverter {
        protected final ColumnHeader header;
        protected final Record row;
        protected final int iColumn;

        public FieldReader(ColumnHeader header, Record row, int iColumn) {
            this.header = header;
            this.row = row;
            this.iColumn = iColumn;
        }
    }

    private static final class LongReader extends FieldReader {
        public LongReader(ColumnHeader header, Record row, int iColumn) {
            super(header, row, iColumn);
        }

        @Override
        public void addLong(long value) {
            row.setLong(iColumn, value);
        }
    }

    private static FieldReader[] fieldReaders(List<ColumnHeader> headers, Record row) {
        final FieldReader[] fieldReaders = new FieldReader[headers.size()];

        int iColumn = 0;
        for (ColumnHeader header : headers) {
            if (header.type() == ColumnHeader.ColumnType.LONG) {
                fieldReaders[iColumn] = new LongReader(header, row, iColumn++);
            }
        }

        return fieldReaders;
    }

    public static List<ColumnHeader> inferByType(MessageType fileSchema) {
        final List<ColumnHeader> headers = newArrayList();

        for (ColumnDescriptor descriptor : fileSchema.getColumns()) {
            final String fieldName = descriptor.getPath()[descriptor.getPath().length - 1];

            if (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64) {
                headers.add(new LongColumnHeader(fieldName));
            } else {
                throw new IllegalArgumentException("Unexpected column type: " + descriptor.getType());
            }
        }

        return headers;
    }
}
