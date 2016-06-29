package net.ndolgov.disruptortest;

import com.lmax.disruptor.WorkHandler;
import java.util.function.Consumer;

/**
 * Parallel data row consumer
 */
public final class DataRowEventConsumer implements WorkHandler<DataRowEvent> {
    private final Consumer<DataRow> consumer;
    private int rowCount = 0;

    public DataRowEventConsumer(Consumer<DataRow> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onEvent(DataRowEvent event) throws Exception {
        consumer.accept(event.row);
        rowCount++;
    }

    public int rowCount() {
        return rowCount;
    }
}
