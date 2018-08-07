package net.ndolgov.akkahttptest.saga.tried

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId, TxContext}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** A saga transaction consists of atomic action and its compensation that can revert the action */
trait ObjectStoreTx {
  def run(ctx: TxContext): Try[Unit]

  def rollback(): Try[Unit]
}

private abstract class WriteToTmpFile(path: String, storage: ObjectStorage) extends ObjectStoreTx {
  private[tried] val logger = LoggerFactory.getLogger(classOf[WriteToTmpFile])

  override final def rollback(): Try[Unit] = storage.deleteFile(path) match {
    case Some(mayBe) =>
      mayBe

    case None =>
      logger.warn(s"File to roll back not found ${path.toString}")
      Success(())
  }
}

/** Create a temporary file from a byte array. Remember output file path and size. */
private[tried] final class WriteArrayToTmpLocation(objId: ObjectId, obj: Array[Byte], path: String, storage: ObjectStorage)
  extends WriteToTmpFile(path, storage) {

  override def run(ctx: TxContext): Try[Unit] = {
    logger.info(s"Storing $objId to $path")

    storage.
      createFile(obj, path).
      map(_ => {
        ctx.setLong(TxContext.Size, obj.length)
        ctx.setString(TxContext.TmpLocation, path)
      })
  }

  override def toString: String = s"WriteArrayToTmpFile(${path.toString})"
}

/** Create a temporary file from a stream source. Remember output file path and size. */
private[tried] final class WriteSourceToTmpLocation(objId: ObjectId, obj: Source[ByteString, Any], path: String, storage: ObjectStorage, timeout: Duration)
                                                   (implicit val materializer: Materializer, implicit val ec: ExecutionContext) extends WriteToTmpFile(path, storage) {

  override def run(ctx: TxContext): Try[Unit] = {
    logger.info(s"Storing $objId to $path")

    val tx = storage.
      createFile(obj, path).
      map(size => {
        ctx.setLong(TxContext.Size, size)
        ctx.setString(TxContext.TmpLocation, path)
      })

    try {
      Await.ready(tx, timeout) // TODO not ready for production
      Success(())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def toString: String = s"WriteSourceToTmpFile(${path.toString})"
}

/** Generate a unique obj revision. Remember the revision. */
private[tried] final class CreateObjectRevision(objId: ObjectId, catalog: ObjectCatalog) extends ObjectStoreTx {
  private var revision: Option[String] = None

  override def run(ctx: TxContext): Try[Unit] = {
    catalog.
      createRevision(objId).
      map(v => {
        revision = Some(v)
        ctx.setString(TxContext.Revision, v)
      })
  }

  override def rollback(): Try[Unit] = catalog.forgetRevision(objId.copy(revision = revision))

  override def toString: String = s"CreateObjectRevision($objId)"
}

/** Move a temporary file to its permanent location */
private[tried] final class MakeTmpFilePermanent(revisionToPath: String => String, storage: ObjectStorage) extends ObjectStoreTx {
  private var tmpPath: String = _
  private var permanentPath: String = _

  override def run(ctx: TxContext): Try[Unit] = {
    tmpPath = ctx.getString(TxContext.TmpLocation)
    permanentPath = revisionToPath(ctx.getString(TxContext.Revision))

    storage.
      renameFile(tmpPath, permanentPath).
      map(_ => ctx.setString(TxContext.PermanentLocation, permanentPath))
  }

  override def rollback(): Try[Unit] = Success(()) // WriteXXXToTmpFile will delete the temporary file

  override def toString: String = s"MakeTmpFilePermanent(${tmpPath.toString} => ${permanentPath.toString})"
}
