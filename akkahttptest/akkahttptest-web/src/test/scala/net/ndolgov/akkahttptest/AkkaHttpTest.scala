package net.ndolgov.akkahttptest

import java.net.URL

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import net.ndolgov.akkahttptest.service.{TestRequestA, TestRequestB, TestResponseA, TestResponseB, TestServiceAImpl, TestServiceBImpl, TestServiceCImpl}
import net.ndolgov.akkahttptest.web.{HttpEndpointA, HttpEndpointB, HttpEndpointC}
import org.scalatest.{Assertions, FlatSpec}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AkkaHttpTest extends FlatSpec with Assertions {
  private val logger = LoggerFactory.getLogger(classOf[AkkaHttpTest])
  private val PORT = 20000
  private val REQUEST_ID = 123
  private val HOSTNAME = "127.0.0.1"
  private val THREADS = 4
  private val TIMEOUT = 10000

  it should "support two service endpoints on the same port" in {
    val serverStartingUp = AkkaHttpServer(HOSTNAME, PORT, Some(serverExecutorSvc())).
      start((ec: ExecutionContext, materializer: Materializer) =>
        new HttpEndpointA(new TestServiceAImpl()(ec), TIMEOUT).endpointRoutes() ~
        new HttpEndpointB(new TestServiceBImpl()(ec), TIMEOUT).endpointRoutes() ~
        new HttpEndpointC(new TestServiceCImpl()(ec, materializer), TIMEOUT).endpointRoutes())
    val server = Await.result(serverStartingUp, Duration.create(TIMEOUT, TimeUnit.MILLISECONDS))

    val client = new AkkaHttpClient(new URL("http", HOSTNAME, PORT, "/akkahttp/test").toString, clientExecutorSvc())

    try {
      val ec: ExecutionContext = ExecutionContext.fromExecutor(client.executor)

      val fa: Future[TestResponseA] = handle(client.callA(TestRequestA(REQUEST_ID)))(ec)
      val fb: Future[TestResponseB] = handle(client.callB(TestRequestB(REQUEST_ID)))(ec)

      Await.ready(fa zip fb, Duration.create(TIMEOUT, TimeUnit.MILLISECONDS))
    } finally {
      Await.ready(server.stop(), Duration.create(TIMEOUT, TimeUnit.MILLISECONDS))
    }
  }

  private def handle[A](future: Future[A])(implicit ec: ExecutionContext): Future[A] = {
    future.onComplete((triedA: Try[A]) => triedA match {
      case Success(response) =>
        logger.info("Processing response: " + response)

      case Failure(e) =>
        logger.error("RPC call failed ", e)
    })

    future
  }

  private def serverExecutorSvc() = {
    Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-server-%d").build())
  }

  private def clientExecutorSvc(): ExecutorService =
    Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-client-%d").build)
}
