package net.ndolgov.akkahttptest.saga.tried

import java.util.concurrent.TimeUnit

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId, TxContext}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ObjectStoreSaga {
  type Saga = () => Try[TxContext]

  private val Timeout = Duration.create(60000, TimeUnit.MILLISECONDS)

  def apply(txs: List[ObjectStoreTx]): Saga = new TryListSaga(txs)

  def apply(objId: ObjectId, obj: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage): Saga = {
    val writeTx = new WriteArrayToTmpLocation(objId, obj, storage.tmpPath(objId), storage)
    apply(writeTx, objId, catalog, storage)
  }

  def apply(objId: ObjectId, obj: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit materializer: Materializer, ec: ExecutionContext): Saga = {
    val writeTx = new WriteSourceToTmpLocation(objId, obj, storage.tmpPath(objId), storage, Timeout)
    apply(writeTx, objId, catalog, storage)
  }

  private def apply(writeTx: ObjectStoreTx, objId: ObjectId, catalog: ObjectCatalog, storage: ObjectStorage): Saga = {
    apply(
      List(
        writeTx,
        new CreateObjectRevision(objId, catalog),
        new MakeTmpFilePermanent(
          v => storage.persistentPath(objId.copy(revision = Option(v))),
          storage)))
  }
}

/** A saga comprised of a linear sequence of ObjectStoreTxs executed on the calling thread */
private final class TryListSaga(txs: List[ObjectStoreTx]) extends Function0[Try[TxContext]] {
  private val logger = LoggerFactory.getLogger(classOf[TryListSaga])

  def apply(): Try[TxContext] = {
    val ctx = TxContext()

    for (txIndex <- txs.indices) {
      val tx = txs(txIndex)

      tx.run(ctx) match {
        case Failure(e) =>
          logger.error(s"Failed to execute ${tx.toString}", e)
          rollback(txIndex)
          return Failure(e)

        case _ =>
          logger.info(s" applied ${tx.toString}")
      }
    }

    Success(ctx)
  }

  private def rollback(failedTxIndex: Int) : Unit = {
    for (i <- failedTxIndex-1 to 0 by -1) {
      val tx = txs(i)

      tx.rollback() match {
        case Failure(e) => logger.error(s"Failed to roll back ${tx.toString}", e)
        case _ => logger.info(s" reverted ${tx.toString}")
      }
    }
  }
}
