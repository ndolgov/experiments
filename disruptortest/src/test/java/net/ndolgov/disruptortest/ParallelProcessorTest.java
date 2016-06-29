package net.ndolgov.disruptortest;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

public final class ParallelProcessorTest {
    @Test
    public void testShortBurst() {
        final ExecutorService producerExecutor = Executors.newSingleThreadExecutor(new DisruptorThreadFactory("producer"));
        final int nProcessors = Runtime.getRuntime().availableProcessors();

        try {
            final ParallelProcessor processor = new ParallelProcessor(producerExecutor, nProcessors, 1_024);
            final ParallelRequestRunner runner = new ParallelRequestRunner(processor, 5_000, 5_000);

            final int expectedCount = 100_000;
            final AtomicInteger count = new AtomicInteger(0);

            runner.run(
                IntStream.rangeClosed(1, expectedCount).mapToObj(i -> new DataRow(i, i)).iterator(),
                new Consumer<DataRow>() {
                    @Override
                    public synchronized void accept(DataRow dataRow) {
                        count.incrementAndGet();
                    }
                });

            assertEquals(count.get(), expectedCount);
        } finally {
            producerExecutor.shutdown();
        }
    }
}
