package net.ndolgov.parquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

import java.util.HashMap;
import java.util.List;

public final class RecordWriteSupport extends WriteSupport<Record> {
    private final List<ColumnHeader> headers;

    private RecordConsumer recordConsumer;

    private WriterFactory.Writer writer;

    public RecordWriteSupport(List<ColumnHeader> headers) {
        this.headers = headers;
    }
    
    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(ToParquet.from(headers), new HashMap<>());
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
    }
}
