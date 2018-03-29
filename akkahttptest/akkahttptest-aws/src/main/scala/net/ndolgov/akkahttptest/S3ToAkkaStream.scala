package net.ndolgov.akkahttptest

import java.nio.ByteBuffer

import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import org.reactivestreams.Publisher
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.core.async.AsyncResponseHandler
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import scala.concurrent.{ExecutionContext, Future, Promise}

trait FileDownloader {
  /**
    * @param path relative file path
    * @return Akka Source that represents an S3 file being download
    */
  def download(path: String): Future[Source[ByteString, Any]]
}

/** Wrap AWS SDK reactive stream Publisher into an Akka Source. */
private final class S3ToAkkaStream(client: S3AsyncClient, bucket: String)(implicit ec: ExecutionContext) extends FileDownloader {
  private val logger = LoggerFactory.getLogger(classOf[S3ToAkkaStream])

  override def download(path: String): Future[Source[ByteString, Any]] = {
    val request = GetObjectRequest.builder().bucket(bucket).key(path).build()

    logger.info(s"Executing ${request.toString}")

    val handler = new AkkaStreamHandler(logger)

    client.
      getObject(request, handler).
      whenComplete((_, e) => {
        if (e == null) {
          logger.info(s"Finished ${request.toString}", e)
        } else {
          logger.error(s"Failed ${request.toString}")
          handler.fail(e)
        }
      })

    handler.source()
  }
}

/** Create a Future that is completed when download starts */
private final class AkkaStreamHandler(logger: Logger) extends AsyncResponseHandler[GetObjectResponse, Unit] {
  private val promise = Promise[Source[ByteString, Any]]()

  def source() : Future[Source[ByteString, Any]] = promise.future

  def fail(e: Throwable) : Unit = promise.failure(e)

  override def onStream(publisher: Publisher[ByteBuffer]): Unit = {
    val src = Source.
      fromPublisher(publisher).
      via(Flow[ByteBuffer].map(bb => ByteString.fromByteBuffer(bb)))

    promise.success(src)
  }

  override def exceptionOccurred(e: Throwable): Unit = {
    fail(e)
  }

  override def responseReceived(response: GetObjectResponse): Unit = {
    logger.info(s"Starting download of ${response.toString}" )
  }

  override def complete(): Unit = {}
}

object S3ToAkkaStream {
  private val PROFILE = "default"

  def apply(region: Region, bucket: String)(implicit ec: ExecutionContext): FileDownloader = {
    val s3client = S3AsyncClient.builder.
      region(region).
//      credentialsProvider(
//        ProfileCredentialsProvider.builder().profileName(PROFILE).build()).
      build

    new S3ToAkkaStream(s3client, bucket)
  }
}

