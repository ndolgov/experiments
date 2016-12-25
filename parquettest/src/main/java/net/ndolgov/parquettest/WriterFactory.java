package net.ndolgov.parquettest;

import org.apache.parquet.io.api.RecordConsumer;

import java.util.List;

public final class WriterFactory {
    public interface Writer {
        void write(Record record);
    }

    public static Writer create(List<ColumnHeader> headers, RecordConsumer consumer) {
        return new GroupWriter(fieldWriters(consumer, headers), consumer);
    }

    private static FieldWriter[] fieldWriters(RecordConsumer consumer, List<ColumnHeader> headers) {
        final FieldWriter[] writers = new FieldWriter[headers.size()];

        for (int i = 0; i < headers.size(); i++) {
            final ColumnHeader header = headers.get(i);
            switch (header.type()) {
                case LONG:
                    writers[i] = new LongWriter(consumer, header.name(), i); break;

                default:
                    throw new IllegalArgumentException("Unexpected header type: " + header.type());
            }
        }

        return writers;
    }

    /**
     * Common for all field types
     */
    private static abstract class FieldWriter {
        protected final RecordConsumer recordConsumer;
        protected final String fieldName;
        protected final int index;

        public FieldWriter(RecordConsumer recordConsumer, String fieldName, int index) {
            this.recordConsumer = recordConsumer;
            this.fieldName = fieldName;
            this.index = index;
        }

        protected abstract boolean isNull(Record record);

        protected abstract void writeValue(Record record);

        protected final void writeField(Record record) {
            if (!isNull(record)) { // NULLs are written implicitly by not having a field written
                recordConsumer.startField(fieldName, index);
                writeValue(record);
                recordConsumer.endField(fieldName, index);
            }
        }
    }

    /**
     * Write a Long field value to Parquet file
     */
    private static final class LongWriter extends FieldWriter {
        public LongWriter(RecordConsumer recordConsumer, String fieldName, int offset) {
            super(recordConsumer, fieldName, offset);
        }

        @Override
        protected final void writeValue(Record record) {
            recordConsumer.addLong(record.getLong(index));
        }

        @Override
        protected boolean isNull(Record record) {
            return record.isNull(index);
        }
    }

    /**
     * Write a record to Parquet file
     */
    private static final class GroupWriter extends FieldWriter implements Writer {
        private final FieldWriter[] fieldWriters;

        @SuppressWarnings("unchecked")
        public GroupWriter(FieldWriter[] fieldWriters, RecordConsumer recordConsumer) {
            super(recordConsumer, "", -1);

            this.fieldWriters = fieldWriters;
        }

        public void write(Record record) {
            for (FieldWriter entry : fieldWriters) {
                entry.writeField(record);
            }
        }

        @Override
        // currently unused; would be necessary in case of nested structures
        protected final void writeValue(Record record) {
            recordConsumer.startGroup();
            write(record);
            recordConsumer.endGroup();
        }

        @Override
        protected boolean isNull(Record record) {
            return false;
        }
    }
}
