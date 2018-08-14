package net.ndolgov.akkahttptest.saga.monadic

import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectStorage, ObjectCatalog, ObjectId}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * A saga transaction comprised of atomic action and its compensation that can revert the action.
  *
  * This monadic approach is based on
  * - https://github.com/winitzki/scala-examples/blob/master/chapter08/src/test/scala/example/TransactionMonad.scala
  * that was inspired by
  * - https://www.fos.kuis.kyoto-u.ac.jp/~igarashi/papers/pdf/contextflow-REBLS17.pdf.
  *
  * See http://blog.tmorris.net/posts/continuation-monad-in-scala/ for a Continuation Monad introduction.
  *
  * See https://stackoverflow.com/questions/6951895/what-does-and-mean-in-scala for "action: =>" meaning refresher.
  */
trait ObjectStoreTx[A] {
  self =>

  type Tx[B, R] = B => Future[R]

  type Continuation[B, R] = (Tx[B, R]) => Future[R]

  def run[R]: Continuation[A, R]

  final def map[B](f: A => B): ObjectStoreTx[B] = new ObjectStoreTx[B] {
    def run[R]: Continuation[B, R] = { g: Tx[B, R] => self.run(f andThen g) }
  }

  final def flatMap[B](f: A => ObjectStoreTx[B]): ObjectStoreTx[B] = new ObjectStoreTx[B] {
    def run[R]: Continuation[B, R] = { g: Tx[B, R] => self.run(f(_).run(g)) }
  }

  final def toFuture: Future[A] = run { a: A => Future.successful(a) }

  // Necessary for Scala for comprehensions (https://stackoverflow.com/questions/1052476/what-is-scalas-yield)
  final def withFilter(p: A => Boolean): ObjectStoreTx[A] = new ObjectStoreTx[A] {
    override def run[R]: Continuation[A, R] = { g: Tx[A, R] =>
      self.run { a =>
        if (p(a)) g(a) else Future.failed(new Exception(s"Invalid match with value $a"))
      }
    }
  }
}

/** Helper functions to convert an action represented by a Future into a saga transaction */
trait ObjectStoreTxs {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Create a tx with a no-op compensation */
  def toTx[A](action: => Future[A])(implicit ec: ExecutionContext): ObjectStoreTx[A] = toRevertableTx(action)(_ => Success(()))

  /** Create a tx from a future (that may fail) and a cleanup function (that may also fail) */
  def toRevertableTx[A](action: => Future[A])(rollback: A => Try[Unit])(implicit ec: ExecutionContext): ObjectStoreTx[A] = new ObjectStoreTx[A] {
    override def run[R]: Continuation[A, R] = (restOfPipeline: Tx[A, R]) =>
      for {
        a <- action
        result <- restOfPipeline(a).transform(identity, { ex => log(rollback(a)); ex })
      } yield result
  }

  final def log[A](mayBe: Try[A]): Unit = mayBe match {
    case Failure(e) => logger.error(s"Failed to roll back", e)
    case _ =>
  }
}

/** Create a temporary file from a stream source or an array. Return output file path and size. */
object WriteToTmpLocation extends ObjectStoreTxs {
  private def writeArrayToTmpLocation(objId: ObjectId, obj: Array[Byte], path: String, storage: ObjectStorage)
                                 (implicit ec: ExecutionContext): Future[(String, Long)] = {
    Future {
      logger.info(s"Storing $objId to $path")

      storage.
        createFile(obj, path).
        map(_ => (path, obj.length.toLong))
    }.
      flatMap(Future.fromTry)
  }

  private def deleteFile(path: String, storage: ObjectStorage): Try[Unit] = storage.deleteFile(path) match {
    case Some(mayBe) =>
      logger.info(s" reverted WriteToTmpLocation($path)")
      mayBe

    case None =>
      logger.warn(s"Failed to roll back WriteToTmpLocation($path)")
      Success(())
  }

  private def writeSourceToTmpLocation(objId: ObjectId, obj: Source[ByteString, Any], path: String, storage: ObjectStorage)
                                  (implicit ec: ExecutionContext): Future[(String, Long)] = {
    logger.info(s"Storing $objId to $path")

    storage.
      createFile(obj, path).
      map(size => (path, size))
  }

  def fromArrayTx(objId: ObjectId, obj: Array[Byte], path: String, storage: ObjectStorage)
                 (implicit ec: ExecutionContext): ObjectStoreTx[(String, Long)] =
    toRevertableTx(writeArrayToTmpLocation(objId, obj, path, storage)) { case (filePath, _) => deleteFile(filePath, storage) }

  def fromSourceTx(objId: ObjectId, obj: Source[ByteString, Any], path: String, storage: ObjectStorage)
                  (implicit ec: ExecutionContext): ObjectStoreTx[(String, Long)] =
    toRevertableTx(writeSourceToTmpLocation(objId, obj, path, storage)) { case (filePath, _) => deleteFile(filePath, storage) }
}

/** Generate a unique obj revision */
object CreateObjectRevision extends ObjectStoreTxs {
  private def createObjectRevision(objId: ObjectId, catalog: ObjectCatalog)(implicit ec: ExecutionContext): Future[String] = {
    Future {
      catalog.createRevision(objId)
    }.
      flatMap(Future.fromTry)
  }

  def tx(objId: ObjectId, catalog: ObjectCatalog)(implicit ec: ExecutionContext): ObjectStoreTx[String] =
    toRevertableTx(createObjectRevision(objId, catalog))(v => catalog.forgetRevision(objId.copy(revision = Some(v))))
}

/** Move a temporary file to its permanent location.  */
object MakeTmpFilePermanent extends ObjectStoreTxs {
  private def renameTmpFile(tmpString: String, permanentString: String, storage: ObjectStorage)
                           (implicit ec: ExecutionContext): Future[String] = {
    Future {
      storage.
        renameFile(tmpString, permanentString).
        map(_ => permanentString)
    }.
      flatMap(Future.fromTry)
  }

  def tx(tmpString: String, ver: String, objId: ObjectId, storage: ObjectStorage)
        (implicit ec: ExecutionContext): ObjectStoreTx[String] =
    toTx(renameTmpFile(tmpString, storage.persistentPath(objId.copy(revision = Option(ver))), storage))
}
