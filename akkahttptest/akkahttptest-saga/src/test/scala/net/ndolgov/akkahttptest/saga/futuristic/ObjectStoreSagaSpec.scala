package net.ndolgov.akkahttptest.saga.futuristic

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectCatalog, ObjectId, ObjectStorage, ObjectStoreSagaAsserions, TxContext}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertions, FlatSpec, Matchers}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext

class ObjectStoreSagaSpec extends FlatSpec with Assertions with MockitoSugar with Matchers {
  private implicit val ec: ExecutionContext = ActorSystem("FuturisticSagaSpec").dispatcher

  private val timeout: FiniteDuration = Duration.create(5000, TimeUnit.MILLISECONDS)

  private val assertions = new ObjectStoreSagaAsserions()(ec)

  it should "execute array saga successfully" in {
    assertions.executeArraySagaSuccessfully(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.executeArraySagaSuccessfully(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

  it should "execute streaming saga successfully" in {
    assertions.executeStreamingSagaSuccessfully(
      (objId: ObjectId, blob: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage) => ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.executeStreamingSagaSuccessfully(
      (objId: ObjectId, blob: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage) => FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

  it should "rollback on renaming error" in {
    assertions.rollbackOnRenamingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.rollbackOnRenamingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

  it should "rollback on revision generation error" in {
    assertions.rollbackOnRevisionGenerationError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.rollbackOnRevisionGenerationError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

  it should "do nothing on file writing error" in {
    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

  it should "continue rollback on undo error" in {
    assertions.continueRollbackOnRollbackError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => ComposedFuturesSaga(objId, blob, catalog, storage)(ec))

    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => FutureSeqSaga(objId, blob, catalog, storage)(ec))
  }

}

