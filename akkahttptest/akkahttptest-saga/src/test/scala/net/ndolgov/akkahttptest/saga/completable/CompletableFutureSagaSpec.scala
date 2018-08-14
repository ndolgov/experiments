package net.ndolgov.akkahttptest.saga.completable

import java.util
import java.util.concurrent.{CompletableFuture, Executor, Executors}

import akka.actor.ActorSystem
import net.ndolgov.akkahttptest.saga.completable.ObjectStoreTxs.{GenerateRevision, MakeFilePermanent, WriteTmpFileTx}
import net.ndolgov.akkahttptest.saga.{ObjectCatalog, ObjectId, ObjectStorage, ObjectStoreSagaAsserions, TxContext}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertions, FlatSpec, Matchers}

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Success

class CompletableFutureSagaSpec extends FlatSpec with Assertions with MockitoSugar with Matchers {
  private val ec: ExecutionContext = ActorSystem("CompletableFutureSagaSpec").dispatcher

  private val assertions = new ObjectStoreSagaAsserions()(ec)

  private val executor: Executor = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())

  it should "execute array saga successfully" in {
    assertions.executeArraySagaSuccessfully(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        toSaga(objId, blob, catalog, storage, txs => () => new CompletableFutureSaga(txs).apply()))
  }

  it should "rollback on renaming error" in {
    assertions.rollbackOnRenamingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        toSaga(objId, blob, catalog, storage, txs => () => new CompletableFutureSaga(txs).apply()))
  }

  it should "rollback on revision generation error" in {
    assertions.rollbackOnRevisionGenerationError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        toSaga(objId, blob, catalog, storage, txs => () => new CompletableFutureSaga(txs).apply()))
  }

  it should "do nothing on file writing error" in {
    assertions.doNothingOnFileWritingError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        toSaga(objId, blob, catalog, storage, txs => () => new CompletableFutureSaga(txs).apply()))
  }

  it should "continue rollback on undo error" in {
    assertions.continueRollbackOnRollbackError(
      (objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage) =>
        toSaga(objId, blob, catalog, storage, txs => () => new CompletableFutureSaga(txs).apply()))
  }

  private def toSaga(objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage,
                     createSaga: ((util.ArrayList[ObjectStoreTx]) => (() => CompletableFuture[TxContext]))) = {
    val p = Promise[TxContext]

    createSaga(transactions(objId, blob, catalog, storage)).apply().whenComplete((ctx: TxContext, e: Throwable) => {
      if (e == null) {
        p.complete(Success(ctx))
      } else {
        p.failure(e)
      }
    })

    () => p.future
  }

  private def transactions(objId: ObjectId, blob: Array[Byte], catalog: ObjectCatalog, storage: ObjectStorage): util.ArrayList[ObjectStoreTx] = {
    val txs: util.ArrayList[ObjectStoreTx] = new util.ArrayList[ObjectStoreTx]()
    txs.add(new WriteTmpFileTx(blob, storage.tmpPath(objId), storage, executor))
    txs.add(new GenerateRevision(objId, catalog, executor))
    txs.add(new MakeFilePermanent(storage, v => storage.persistentPath(objId.copy(revision = Option(v))), executor))
    txs
  }
}

