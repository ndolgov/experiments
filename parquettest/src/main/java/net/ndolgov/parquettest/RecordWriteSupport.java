package net.ndolgov.parquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

import java.util.List;
import java.util.Map;

public final class RecordWriteSupport extends WriteSupport<Record> {
    static final String ROW_COUNT = "ROW_COUNT";

    private final List<ColumnHeader> headers;

    private final Map<String, String> metadata;

    private RecordConsumer recordConsumer;

    private WriterFactory.Writer writer;

    private long rowCount;

    public RecordWriteSupport(List<ColumnHeader> headers, Map<String, String> metadata) {
        this.headers = headers;
        this.metadata = metadata;
    }
    
    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(ToParquet.from(headers), metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer consumer) {
        this.recordConsumer = consumer;
        this.writer = WriterFactory.create(headers, consumer);
    }

    @Override
    public void write(Record record) {
        recordConsumer.startMessage();

        try {
            writer.write(record);
        } catch (RuntimeException e) {
            throw new RuntimeException("Could not write record: " + record, e);
        }

        recordConsumer.endMessage();
        rowCount++;
    }

    @Override
    public FinalizedWriteContext finalizeWrite() {
        metadata.put(ROW_COUNT, String.valueOf(rowCount));
        return new FinalizedWriteContext(metadata);
    }
}
