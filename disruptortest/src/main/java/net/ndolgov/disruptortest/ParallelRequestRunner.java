package net.ndolgov.disruptortest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Placeholder for an actual client. The run method is supposed to be called for every request. The caller is
 * expected to process potential runtime exceptions.
 */
public final class ParallelRequestRunner {
    private static final Logger logger = LoggerFactory.getLogger(ParallelRequestRunner.class);

    private final ParallelProcessor processor;

    private final long processingTimeout;

    private final long shutdownTimeout;

    public ParallelRequestRunner(ParallelProcessor processor, long processingTimeout, long shutdownTimeout) {
        this.processor = processor;
        this.processingTimeout = processingTimeout;
        this.shutdownTimeout = shutdownTimeout;
    }

    public void run(Iterator<DataRow> input, Consumer<DataRow> output) {
        final ParallelProcessingContext ctx = processor.process(input, output, processingTimeout, shutdownTimeout);

        if (ctx.await()) {
            logger.info("Producer finished, waiting for consumers");

            ctx.shutDown();
            logger.info("Consumers finished after processing rows: " + ctx.totalRowCount());
        }
    }
}
