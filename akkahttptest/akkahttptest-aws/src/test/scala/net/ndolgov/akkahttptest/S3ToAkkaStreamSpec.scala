package net.ndolgov.akkahttptest

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import org.scalatest.{Assertions, FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

// see also https://ryftcloud.zendesk.com/hc/en-us/articles/115009913207-Get-Sample-Data-from-AWS-S3-Bucket
class S3ToAkkaStreamSpec extends FlatSpec with Assertions with Matchers {
  private val logger = LoggerFactory.getLogger(classOf[S3ToAkkaStreamSpec])

  private val dest = Paths.get(s"target/scala-2.12/test-classes/testfile${System.currentTimeMillis()}.bin")

  private val region = Region.US_EAST_1.toString
  private val bucket = "ryft-public-sample-data"
  private val s3path = "ODBC/SampleDatabases.tar.gz"

  private implicit val actorSystem: ActorSystem = ActorSystem("test-actor-system")
  private implicit val materializer: ActorMaterializer = ActorMaterializer()(actorSystem)
  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  it should "download a public file" in {
  val future = S3ToAkkaStream(Region.of(region), bucket).
    download(s3path).
    flatMap(src => {
        logger.info(s"Writing to ${dest.toAbsolutePath.toString}")
        src.runWith(FileIO.toPath(dest))
    })

    Await.result(future, Duration.create(30, TimeUnit.SECONDS))
    Files.size(dest) shouldEqual 759548

    Files.delete(dest)
  }
}
