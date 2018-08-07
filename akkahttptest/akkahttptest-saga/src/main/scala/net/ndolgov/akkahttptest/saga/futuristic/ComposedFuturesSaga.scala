package net.ndolgov.akkahttptest.saga.futuristic

import net.ndolgov.akkahttptest.saga.TxContext
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ComposedFuturesSaga extends AbstractSaga {
  def apply(txs: Seq[ObjectStoreTx])(implicit ec: ExecutionContext): Saga = new ComposedFuturesSaga(txs)
}

/**
  * Recursively traverse a sequence of transactions to compose the corresponding sequence of Futures.
  * In each Future try to execute the head tx. In case of a failure revert the previously executed transactions. */
private[futuristic] final class ComposedFuturesSaga(txs: Seq[ObjectStoreTx])
                                                   (implicit val ec: ExecutionContext) extends Function0[Future[TxContext]] {

  private val logger = LoggerFactory.getLogger(classOf[ComposedFuturesSaga])

  def apply(): Future[TxContext] = {
    executeHeadTx(txs, TxContext())
  }

  private def executeHeadTx(txs: Seq[ObjectStoreTx], ctx: TxContext) : Future[TxContext] = {
    txs match {
      case Nil => Future.successful(ctx)

      case tx :: tail =>
        tx.
          execute(ctx).
          flatMap { _ =>
            logger.info(s" applied ${tx.toString}")
            executeHeadTx(tail, ctx)
          }.
          recoverWith { case e: Exception =>
            e match {
              case _: NestedTxException => // the actual failed tx has been logged
              case _ => logger.error(s"Failed to execute ${tx.toString}")
            }

            tx.undo() match {
              case Success(_) => logger.info(s" reverted ${tx.toString}")
                
              case Failure(ue) => logger.error(s"Failed to roll back ${tx.toString}", ue)
            }

            Future.failed(new NestedTxException(e))
          }
      }
  }

  /** To distinguish between the actual error happening and its propagation through the chain of Futures */
  private final class NestedTxException(e: Exception) extends RuntimeException(e)
}
