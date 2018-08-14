package net.ndolgov.akkahttptest.saga.completable;

import net.ndolgov.akkahttptest.saga.TxContext;
import net.ndolgov.akkahttptest.saga.TxContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Iterate a sequence of transactions to compose the corresponding sequence of CompletableFutures.
 * In each future try to execute the head tx. In case of a failure revert the previously executed transactions.
 */
public final class CompletableFutureSaga {
    private static Logger logger = LoggerFactory.getLogger(CompletableFutureSaga.class);

    private final List<ObjectStoreTx> transactions;

    public CompletableFutureSaga(List<ObjectStoreTx> transactions) {
        this.transactions = transactions;
    }

    public CompletableFuture<TxContext> apply() {
        return executeHeadTx(transactions.iterator(), new TxContextImpl(), this::rollback);
    }

    private Void rollback(ObjectStoreTx failedTx) {
        for (int txIndex = transactions.indexOf(failedTx) - 1; txIndex >= 0; txIndex--) {
            final ObjectStoreTx tx = transactions.get(txIndex);

            final Optional<Throwable> e = tx.rollback();
            if (e.isPresent()) {
                logger.error("Failed to roll back " + tx, e.get());
            } else {
                logger.debug(" reverted " + tx);
            }
        }

        return null;
    }

    private static CompletableFuture<TxContext> executeHeadTx(Iterator<ObjectStoreTx> txs, TxContext ctx, Consumer<ObjectStoreTx> rollback) {
        if (!txs.hasNext()) {
            return CompletableFuture.completedFuture(ctx);
        }

        final ObjectStoreTx tx = txs.next();

        return tx.
            execute(ctx).
            exceptionally(e -> {
              logger.error("Failed to execute " + tx, e);

              rollback.accept(tx);

              throw new RuntimeException("Cancelling saga after tx failure");
            }).
            thenCompose(unit -> {
              logger.debug(" applied " + tx);

              return executeHeadTx(txs, ctx, rollback);
            });
    }
}


