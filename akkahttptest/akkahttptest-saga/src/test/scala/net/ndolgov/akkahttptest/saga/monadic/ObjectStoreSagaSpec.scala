package net.ndolgov.akkahttptest.saga.monadic

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.ndolgov.akkahttptest.saga.{ObjectCatalog, ObjectId, ObjectStorage, ObjectStoreSagaAsserions, TxContext}
import org.scalatest.{Assertions, FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.ExecutionContext

class ObjectStoreSagaSpec extends FlatSpec with Assertions with MockitoSugar with Matchers {
  private implicit val system: ActorSystem = ActorSystem("MonadicSagaSpec")
  private implicit val ec: ExecutionContext = system.dispatcher

  private val assertions = new ObjectStoreSagaAsserions()(ec)

  it should "execute array saga successfully" in {
    assertions.executeArraySagaSuccessfully(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }

  it should "execute streaming saga successfully" in {
    assertions.executeStreamingSagaSuccessfully(
      (objId: ObjectId, blob: Source[ByteString, Any], catalog: ObjectCatalog, storage: ObjectStorage) => 
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }

  it should "rollback on renaming error" in {
    assertions.rollbackOnRenamingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => 
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }

  it should "rollback on revision generation error" in {
    assertions.rollbackOnRevisionGenerationError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => 
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }

  it should "do nothing on file writing error" in {
    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => 
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }

  it should "continue rollback on undo error" in {
    assertions.continueRollbackOnRollbackError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) => 
        () => ObjectStoreSaga(objId, blob, catalog, storage)(ec).map(toCtx))
  }
  
  private def toCtx(t: (String, Long, String)): TxContext = {
    val ctx = TxContext()
    ctx.setString(TxContext.PermanentLocation, t._1)
    ctx.setLong(TxContext.Size, t._2)
    ctx.setString(TxContext.Revision, t._3)
    ctx
  }
}

