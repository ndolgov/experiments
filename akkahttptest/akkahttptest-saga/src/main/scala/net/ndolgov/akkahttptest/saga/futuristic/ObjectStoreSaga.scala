package net.ndolgov.akkahttptest.saga.futuristic

import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId, TxContext}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectStoreSaga {
  type Saga = (() => Future[TxContext])

  def apply(txs: Seq[ObjectStoreTx])(implicit ec: ExecutionContext): Saga

  def apply(objId: ObjectId, obj: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Saga

  def apply(objId: ObjectId, obj: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Saga
}

/** A means of executing a unit of work comprised of multiple atomic transactions.
  * A transaction failure is handled by applying compensating actions for the transactions that have been executed. */
private[futuristic] abstract class AbstractSaga extends ObjectStoreSaga {
  override def apply(objId: ObjectId, obj: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Saga = {
    val writeTx = new WriteArrayToTmpLocation(objId, obj, storage.tmpPath(objId), storage)
    apply(writeTx, objId, catalog, storage)
  }

  override def apply(objId: ObjectId, obj: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Saga = {
    val writeTx = new WriteSourceToTmpLocation(objId, obj, storage.tmpPath(objId), storage)
    apply(writeTx, objId, catalog, storage)
  }

  private def apply(writeTx: ObjectStoreTx, objId: ObjectId, catalog: ObjectCatalog, storage: ObjectStorage)
                   (implicit ec: ExecutionContext): Saga = {
    apply(
      List(
        writeTx,
        new CreateObjectRevision(objId, catalog),
        new MakeTmpFilePermanent(
          v => storage.persistentPath(objId.copy(revision = Option(v))),
          storage)))
  }
}