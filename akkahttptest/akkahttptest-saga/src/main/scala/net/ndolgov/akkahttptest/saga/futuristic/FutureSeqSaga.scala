package net.ndolgov.akkahttptest.saga.futuristic

import java.util.concurrent.atomic.AtomicInteger

import net.ndolgov.akkahttptest.saga.TxContext
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object FutureSeqSaga extends AbstractSaga {
  def apply(txs: Seq[ObjectStoreTx])(implicit ec: ExecutionContext): Saga = new FutureSeqSaga(txs)
}

/** A saga comprised of a linear sequence of transactions, each to be executed one by one but potentially on different threads. */
private[futuristic] class FutureSeqSaga(txs: Seq[ObjectStoreTx])
                                       (implicit val ec: ExecutionContext) extends Function0[Future[TxContext]] {
  private val logger = LoggerFactory.getLogger(classOf[FutureSeqSaga])
  private val txIndex = new AtomicInteger(0)

  def apply(): Future[TxContext] = {
    val endOfSagaSignal = Promise[TxContext]()

    Future {
      run(txs.head, TxContext(), endOfSagaSignal)
    }

    endOfSagaSignal.future
  }

  /**
    * Run a saga tx. If successful, either trigger next tx execution or signal the end of saga.
    * In case of a failure, trigger rollback of the executed txs.
    * @param tx the transaction to execute
    * @param ctx the state to hand off to the next transaction
    * @param endOfSagaSignal the promise to complete at the end of the saga
    */
  private def run(tx: ObjectStoreTx, ctx: TxContext, endOfSagaSignal: Promise[TxContext]) : Unit = {
    tx.
      execute(ctx).
      onComplete {
        case Success(_) =>
          logger.info(s" applied ${tx.toString}")
          
          val nextIndex = txIndex.incrementAndGet()

          if (nextIndex < txs.length) {
            run(txs(nextIndex), ctx, endOfSagaSignal)
          } else {
            endOfSagaSignal.success(ctx)
          }

        case Failure(e) =>
          logger.error(s"Failed to execute ${tx.toString}", e)

          rollback(txIndex.get())

          endOfSagaSignal.failure(e)
      }
  }

  /** Revert previously executed stages but applying the corresponding compensation actions (in reverse order) */
  private def rollback(failedTxIndex: Int) : Unit = {
    for (i <- failedTxIndex-1 to 0 by -1) {
      val tx = txs(i)

      tx.undo() match {
        case Success(_) => logger.info(s" reverted ${tx.toString}")  
          
        case Failure(e) => logger.error(s"Failed to roll back ${tx.toString}", e)
      }
    }
  }
}
