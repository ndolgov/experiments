package net.ndolgov.akkahttptest.saga

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{ArgumentMatchers => AM}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertions, Matchers}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class ObjectStoreSagaAsserions(implicit val ec: ExecutionContext) extends Assertions with MockitoSugar with Matchers {
  private val Blob = Array.ofDim[Byte](100)
  private val ObjId = ObjectId("someone", None)

  private val Timeout: FiniteDuration = Duration.create(5000, TimeUnit.MILLISECONDS)

  private val TmpPath = "/tmp"

  private val PermPath = "/path"

  private val Revision = "R42"

  private type ArrayToSaga = (ObjectId, Array[Byte], ObjectCatalog, ObjectStorage) => (() => Future[TxContext])

  private type SourceToSaga = (ObjectId, Source[ByteString, Any], ObjectCatalog, ObjectStorage) => (() => Future[TxContext])

  def executeArraySagaSuccessfully(factory: ArrayToSaga) {
    val catalog = mock[ObjectCatalog]
    when(catalog.createRevision(AM.any())).thenReturn(Success(Revision))

    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.persistentPath(AM.any())).thenReturn(PermPath)
    when(storage.createFile(AM.any[Array[Byte]](), AM.any())).thenReturn(Success(()))
    when(storage.renameFile(AM.any(), AM.any())).thenReturn(Success(()))

    val saga = factory(ObjId, Blob, catalog, storage)
    val future = saga().map((ctx: TxContext) => {
      ctx.getString(TxContext.Revision) shouldEqual Revision
      ctx.getLong(TxContext.Size) shouldEqual Blob.length
      ctx.getString(TxContext.PermanentLocation) shouldEqual PermPath
    })

    Await.result(future, Timeout)
  }

  def executeStreamingSagaSuccessfully(factory: SourceToSaga) {
    val catalog = mock[ObjectCatalog]
    when(catalog.createRevision(AM.any())).thenReturn(Success(Revision))

    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.persistentPath(AM.any())).thenReturn(PermPath)
    when(storage.createFile(AM.any[Source[ByteString, Any]](), AM.any())).thenReturn(Future.successful(Blob.length.toLong))
    when(storage.renameFile(AM.any(), AM.any())).thenReturn(Success(()))

    val saga = factory(ObjId, StreamConverters.fromInputStream(() => new ByteArrayInputStream(Blob)), catalog, storage)
    val future = saga().map((ctx: TxContext) => {
      ctx.getString(TxContext.Revision) shouldEqual Revision
      ctx.getLong(TxContext.Size) shouldEqual Blob.length
      ctx.getString(TxContext.PermanentLocation) shouldEqual PermPath
    })

    Await.result(future, Timeout)
  }

  def rollbackOnRenamingError(factory: ArrayToSaga) {
    val catalog = mock[ObjectCatalog]
    when(catalog.createRevision(AM.any())).thenReturn(Success(Revision))
    when(catalog.forgetRevision(AM.any())).thenReturn(Success(()))

    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.persistentPath(AM.any())).thenReturn(PermPath)
    when(storage.createFile(AM.any[Array[Byte]](), AM.any())).thenReturn(Success(()))
    when(storage.renameFile(AM.any(), AM.any())).thenReturn(Failure(new RuntimeException("Simulating rename error")))
    when(storage.deleteFile(AM.any())).thenReturn(Some(Success(())))

    val saga = factory(ObjId, Blob, catalog, storage)
    Await.ready(saga(), Timeout)
    verify(storage, times(1)).deleteFile(AM.any())
    verify(catalog, times(1)).forgetRevision(AM.any())
  }

  def rollbackOnRevisionGenerationError(factory: ArrayToSaga) {
    val catalog = mock[ObjectCatalog]
    when(catalog.createRevision(AM.any())).thenReturn(Failure(new RuntimeException("Simulating revision generation error")))

    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.createFile(AM.any[Array[Byte]](), AM.any())).thenReturn(Success(()))
    when(storage.deleteFile(AM.any())).thenReturn(Some(Success(())))

    val saga = factory(ObjId, Blob, catalog, storage)
    Await.ready(saga(), Timeout)

    verify(storage, times(1)).deleteFile(AM.any())
  }

  def doNothingOnFileWritingError(factory: ArrayToSaga) {
    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.createFile(AM.any[Array[Byte]](), AM.any())).thenReturn(Failure(new RuntimeException("Simulating file writing error")))

    val saga = factory(ObjId, Blob, mock[ObjectCatalog], storage)
    Await.ready(saga(), Timeout)
  }

  def continueRollbackOnRollbackError(factory: ArrayToSaga) {
    val catalog = mock[ObjectCatalog]
    when(catalog.createRevision(AM.any())).thenReturn(Success(Revision))
    when(catalog.forgetRevision(AM.any())).thenReturn(Failure(new RuntimeException("Simulating undo error")))

    val storage = mock[ObjectStorage]
    when(storage.tmpPath(AM.any())).thenReturn(TmpPath)
    when(storage.persistentPath(AM.any())).thenReturn(PermPath)
    when(storage.createFile(AM.any[Array[Byte]](), AM.any())).thenReturn(Success(()))
    when(storage.renameFile(AM.any(), AM.any())).thenReturn(Failure(new RuntimeException("Simulating rename error")))
    when(storage.deleteFile(AM.any())).thenReturn(Some(Success(())))

    val saga = factory(ObjId, Blob, catalog, storage)
    Await.ready(saga(), Timeout)
    verify(storage, times(1)).deleteFile(AM.any())
    verify(catalog, times(1)).forgetRevision(AM.any())
  }

}

