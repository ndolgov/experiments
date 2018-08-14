package net.ndolgov.akkahttptest.saga.completable;

import net.ndolgov.akkahttptest.saga.ObjectCatalog;
import net.ndolgov.akkahttptest.saga.ObjectId;
import net.ndolgov.akkahttptest.saga.ObjectStorage;
import net.ndolgov.akkahttptest.saga.TxContext;
import net.ndolgov.akkahttptest.saga.TxContext$;
import scala.Option;
import scala.runtime.BoxedUnit;
import scala.util.Failure;
import scala.util.Try;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/** A saga transaction consists of atomic action and its compensation that can revert the action */
interface ObjectStoreTx {
    CompletableFuture<Void> execute(TxContext ctx);

    Optional<Throwable> rollback();
}

/** Create a temporary file from a byte array. Remember output file path and size. */
public final class ObjectStoreTxs {
    public static final class WriteTmpFileTx implements ObjectStoreTx {
        private final byte[] obj;
        private final String path;
        private final ObjectStorage storage;
        private final Executor executor;

        public WriteTmpFileTx(byte[] obj, String path, ObjectStorage storage, Executor executor) {
            this.obj = obj;
            this.path = path;
            this.storage = storage;
            this.executor = executor;
        }

        @Override
        public CompletableFuture<Void> execute(TxContext ctx) {
            return CompletableFuture.runAsync(() -> {
                final Try<BoxedUnit> tri = storage.createFile(obj, path);
                if (tri.isFailure()) {
                    throw new RuntimeException(((Failure) tri).exception());
                } else {
                    ctx.setLong(TxContext$.MODULE$.Size(), obj.length);
                    ctx.setString(TxContext$.MODULE$.TmpLocation(), path);
                }
            }, executor);
        }

        @Override
        public Optional<Throwable> rollback() {
            final Option<Try<BoxedUnit>> maybe = storage.deleteFile(path);

            if (maybe.isDefined()) {
                final Try<BoxedUnit> tri = maybe.get();
                if (tri.isFailure()) {
                    return Optional.of(((Failure) tri).exception());
                }
            }

            return Optional.empty();
        }

        @Override
        public String toString() {
            return "WriteTmpFileTx(" + path + ")";
        }
    }

    /** Generate a unique obj revision. Remember the revision. */
    public static final class GenerateRevision implements ObjectStoreTx {
        private final ObjectId objId;
        private final ObjectCatalog catalog;
        private final Executor executor;
        private Option<String> revision = Option.empty();

        public GenerateRevision(ObjectId objId, ObjectCatalog catalog, Executor executor) {
            this.objId = objId;
            this.catalog = catalog;
            this.executor = executor;
        }

        @Override
        public CompletableFuture<Void> execute(TxContext ctx) {
            return CompletableFuture.runAsync(() -> {
                final Try<String> tri = catalog.createRevision(objId);
                if (tri.isFailure()) {
                    throw new RuntimeException(((Failure) tri).exception());
                } else {
                    revision = catalog.createRevision(objId).toOption();
                    ctx.setString(TxContext$.MODULE$.Revision(), revision.get());
                }
            }, executor);
        }

        @Override
        public Optional<Throwable> rollback() {
            final ObjectId id = new ObjectId(objId.name(), revision);
            final Try<BoxedUnit> tri = catalog.forgetRevision(id);
            if (tri.isFailure()) {
                return Optional.of(((Failure) tri).exception());
            } else {
                return Optional.empty();
            }
        }

        @Override
        public String toString() {
            return "CreateObjectRevision(" + objId + ")";
        }
    }

    /** Move a temporary file to its permanent location.  */
    public static final class MakeFilePermanent implements ObjectStoreTx {
        private final ObjectStorage storage;
        private final Function<String, String> revisionToPath;
        private final Executor executor;
        private String tmpPath;
        private String permanentPath;

        public MakeFilePermanent(ObjectStorage storage, Function<String, String> revisionToPath, Executor executor) {
            this.storage = storage;
            this.revisionToPath = revisionToPath;
            this.executor = executor;
        }

        @Override
        public CompletableFuture<Void> execute(TxContext ctx) {
            return CompletableFuture.runAsync(() -> {
                tmpPath = ctx.getString(TxContext$.MODULE$.TmpLocation());
                permanentPath = revisionToPath.apply(ctx.getString(TxContext$.MODULE$.Revision()));

                final Try<BoxedUnit> tri = storage.renameFile(tmpPath, permanentPath);
                if (tri.isFailure()) {
                    throw new RuntimeException(((Failure) tri).exception());
                } else {
                    ctx.setString(TxContext$.MODULE$.PermanentLocation(), permanentPath);
                }
            }, executor);
        }

        @Override
        public Optional<Throwable> rollback() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "MakeFilePermanent(" + tmpPath + "=>" + permanentPath + ")";
        }
    }
}