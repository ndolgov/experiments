package net.ndolgov.akkahttptest.saga.monadic

import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId}

import scala.concurrent.{ExecutionContext, Future}

/**
  * A means of executing a unit of work comprised of multiple atomic transactions.
  * A tx failure is handled by applying compensating actions for the stages that have been executed.
  */
object ObjectStoreSaga {
  type SagaResult = (String, Long, String)

  def apply(objId: ObjectId, obj: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Future[SagaResult] = {
    val writeTx = WriteToTmpLocation.fromArrayTx(objId, obj, storage.tmpPath(objId), storage)
    apply(writeTx, objId, catalog, storage)
  }

  def apply(objId: ObjectId, obj: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage)
           (implicit ec: ExecutionContext): Future[SagaResult] = {
    val writeTx = WriteToTmpLocation.fromSourceTx(objId, obj, storage.tmpPath(objId), storage)
    apply(writeTx, objId, catalog, storage)
  }

  private def apply(writeTx: ObjectStoreTx[(String, Long)], objId: ObjectId, catalog: ObjectCatalog, storage: ObjectStorage)
                   (implicit ec: ExecutionContext): Future[SagaResult] = {
    val saga: ObjectStoreTx[SagaResult] = for {
      (tmpLocation, fileSize) <- writeTx
      revision <- CreateObjectRevision.tx(objId, catalog)
      permanentPath <- MakeTmpFilePermanent.tx(tmpLocation, revision, objId, storage)
    } yield (permanentPath, fileSize, revision)

    saga.toFuture
  }
}
