package net.ndolgov.disruptortest;

import com.lmax.disruptor.EventFactory;

/**
 * Ring buffer slot type
 */
public final class DataRowEvent {
    public final DataRow row;

    private DataRowEvent(DataRow row) {
        this.row = row;
    }

    public void from(DataRow buffer) {
        row.someField = buffer.someField;
        row.anotherField = buffer.anotherField;
    }

    public static final class DataRowEventFactory implements EventFactory<DataRowEvent> {
        @Override
        public DataRowEvent newInstance() {
            return new DataRowEvent(new DataRow());
        }
    }
}
