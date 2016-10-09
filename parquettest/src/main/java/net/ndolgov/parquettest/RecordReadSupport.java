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
public final class RecordReadSupport extends ReadSupport<MutableRecord> {
    private final Map<String, String> metadata = new HashMap<>(32);

    @Override
    public ReadContext init(InitContext context) {
        metadata.putAll(context.getMergedKeyValueMetaData());
        return new ReadContext(context.getFileSchema(), context.getMergedKeyValueMetaData());
    }

    @Override
    public RecordMaterializer<MutableRecord> prepareForRead(Configuration configuration,
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

    private static final class MessageReader extends RecordMaterializer<MutableRecord> {
        private final PrimitiveTypeRowGroupConverter root;
        private final MutableRecord record;

        public MessageReader(List<ColumnHeader> headers) {
            record = new MutableRecord();
            root = new PrimitiveTypeRowGroupConverter(fieldReaders(headers, record));
        }

        @Override
        public MutableRecord getCurrentRecord() {
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
        protected final MutableRecord row;

        public FieldReader(ColumnHeader header, MutableRecord row) {
            this.header = header;
            this.row = row;
        }
    }

    private static final class LongReader extends FieldReader {
        public LongReader(ColumnHeader header, MutableRecord row) {
            super(header, row);
        }

        @Override
        public void addLong(long value) {
            row.value(value);
        }
    }

    private static FieldReader[] fieldReaders(List<ColumnHeader> headers, MutableRecord row) {
        final FieldReader[] fieldReaders = new FieldReader[headers.size()];

        int iColumn = 0;
        for (ColumnHeader header : headers) {
            if (header.type() == ColumnHeader.ColumnType.LONG) {
                fieldReaders[iColumn++] = new LongReader(header, row);
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
