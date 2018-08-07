package net.ndolgov.akkahttptest.saga.tried

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectCatalog, ObjectId, ObjectStorage, ObjectStoreSagaAsserions}
import org.scalatest.{Assertions, FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ExecutionContext, Future}

class ObjectStoreSagaSpec extends FlatSpec with Assertions with MockitoSugar with Matchers {
  private implicit val system: ActorSystem = ActorSystem("TriedSagaSpec")
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()

  private val assertions = new ObjectStoreSagaAsserions()(ec)
  
  it should "execute array saga successfully" in {
    assertions.executeArraySagaSuccessfully(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)()))
  }

  it should "execute streaming saga successfully" in {
    assertions.executeStreamingSagaSuccessfully(
      (objId: ObjectId, blob: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)(materializer, ec)()))
  }

  it should "rollback on renaming error" in {
    assertions.rollbackOnRenamingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)()))
  }

  it should "rollback on revision generation error" in {
    assertions.rollbackOnRevisionGenerationError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)()))
  }

  it should "do nothing on file writing error" in {
    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)()))
  }

  it should "continue rollback on undo error" in {
    assertions.continueRollbackOnRollbackError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => Future.fromTry(ObjectStoreSaga(objId, blob, catalog, storage)()))
  }
}

