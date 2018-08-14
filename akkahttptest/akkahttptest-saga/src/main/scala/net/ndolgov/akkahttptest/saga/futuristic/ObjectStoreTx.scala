package net.ndolgov.akkahttptest.saga.futuristic

import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId, TxContext}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/** A saga transaction consists of atomic action and its compensation that can revert the action */
trait ObjectStoreTx {
  def execute(ctx: TxContext): Future[Unit]

  def rollback(): Try[Unit]
}

private abstract class WriteToTmpLocation(path: String, storage: ObjectStorage) extends ObjectStoreTx {
  private[futuristic] val logger = LoggerFactory.getLogger(classOf[WriteToTmpLocation])

  override final def rollback(): Try[Unit] = storage.deleteFile(path) match {
    case Some(mayBe) =>
      mayBe

    case None =>
      logger.warn(s"File to roll back not found ${path.toString}")
      Success(())
  }
}

/** Create a temporary file from a byte array. Remember output file path and size. */
private[futuristic] final class WriteArrayToTmpLocation(objId: ObjectId, obj: Array[Byte], path: String, storage: ObjectStorage)
                                                       (implicit val ec: ExecutionContext) extends WriteToTmpLocation(path, storage) {

  override def execute(ctx: TxContext): Future[Unit] = Future {
    logger.info(s"Storing $objId to $path")

    storage.
      createFile(obj, path).
      map(_ => {
        ctx.setLong(TxContext.Size, obj.length)
        ctx.setString(TxContext.TmpLocation, path)
      })
  }.
    flatMap(Future.fromTry)

  override def toString: String = s"WriteArrayToTmpLocation(${path.toString})"
}

/** Create a temporary file from a stream source. Remember output file path and size. */
private[futuristic] final class WriteSourceToTmpLocation(objId: ObjectId, obj: Source[ByteString, Any], path: String, storage: ObjectStorage)
                                                        (implicit val ec: ExecutionContext) extends WriteToTmpLocation(path, storage) {

  override def execute(ctx: TxContext): Future[Unit] = {
    logger.info(s"Storing $objId to $path")

    storage.
      createFile(obj, path).
      map(size => {
        ctx.setLong(TxContext.Size, size)
        ctx.setString(TxContext.TmpLocation, path)
      })
  }

  override def toString: String = s"WriteSourceToTmpLocation(${path.toString})"
}

/** Generate a unique obj revision. Remember the revision. */
private[futuristic] final class CreateObjectRevision(objId: ObjectId, catalog: ObjectCatalog)
                                                    (implicit val ec: ExecutionContext) extends ObjectStoreTx {
  private var revision: Option[String] = None

  override def execute(ctx: TxContext): Future[Unit] = Future {
    catalog.
      createRevision(objId).
      map(v => {
        revision = Some(v)
        ctx.setString(TxContext.Revision, v)
      })
  }.
    flatMap(Future.fromTry)

  override def rollback(): Try[Unit] = catalog.forgetRevision(objId.copy(revision = revision))

  override def toString: String = s"CreateObjectRevision($objId)"
}

/** Move a temporary file to its permanent location.  */
private[futuristic] final class MakeTmpFilePermanent(revisionToPath: String => String, storage: ObjectStorage)
                                                    (implicit val ec: ExecutionContext) extends ObjectStoreTx {
  private var tmpPath: String = _
  private var permanentPath: String = _

  override def execute(ctx: TxContext): Future[Unit] = Future {
    tmpPath = ctx.getString(TxContext.TmpLocation)
    permanentPath = revisionToPath(ctx.getString(TxContext.Revision))

    storage.
      renameFile(tmpPath, permanentPath).
      map(_ => ctx.setString(TxContext.PermanentLocation, permanentPath))
  }.
    flatMap(Future.fromTry)

  override def rollback(): Try[Unit] = Success(()) // WriteXXXToTmpFile will delete the temporary file

  override def toString: String = s"MakeTmpFilePermanent(${tmpPath.toString} => ${permanentPath.toString})"
}
