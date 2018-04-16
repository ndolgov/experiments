package net.ndolgov.akkahttptest.service

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import akka.stream.{IOResult, Materializer}
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

case class TestResponseC(key: String)

/** Service API illustrating Akka stream API usage */
trait TestServiceC {
  def process(key: String, stream: Source[ByteString, Any]) : Future[TestResponseC]

  def process(key: String) : Future[Source[ByteString, Future[IOResult]]]
}

final class TestServiceCImpl(implicit val ec: ExecutionContext, implicit val materializer: Materializer) extends TestServiceC {
  private val logger = LoggerFactory.getLogger(classOf[TestServiceCImpl])

  override def process(key: String, stream: Source[ByteString, Any]): Future[TestResponseC] = {
    val promise = Promise[TestResponseC]()

    Future {
      logger.info("Computing result")
      stream.
        runWith(StreamConverters.fromOutputStream(() => new ByteArrayOutputStream())).
        onComplete {
          case Success(_) =>
            promise.complete(Success(TestResponseC(key)))

          case Failure(e) =>
            promise.failure(new RuntimeException(s"Failed to create $key from a stream", e))
        }
    }

    promise.future
  }

  override def process(key: String): Future[Source[ByteString, Future[IOResult]]] = Future {
    StreamConverters.fromInputStream(() => new ByteArrayInputStream(new Array(0)))
  }
}