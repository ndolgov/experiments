package net.ndolgov.scalapbtest

import org.scalatest.{Assertions, FlatSpec}
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import net.ndolgov.scalapbtest.api.testsvcA.{TestRequestA, TestServiceAGrpc}
import net.ndolgov.scalapbtest.api.testsvcB.{TestRequestB, TestServiceBGrpc}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AsyncScalaPBCallTest extends FlatSpec with Assertions {
  private val logger = LoggerFactory.getLogger(classOf[AsyncScalaPBCallTest])
  private val PORT = 20000
  private val REQUEST_ID = 123
  private val HOSTNAME = "127.0.0.1"
  private val THREADS = 4
  private val RESULT = "RESULT"

  it should "support two service endpoints on the same port" in {
    val server = startServer(ExecutionContext.fromExecutor(serverExecutorSvc()))
    val client = GrpcClient(HOSTNAME, PORT, clientExecutorSvc())

    try {
      implicit val clientExecutionContext: ExecutionContext = client.executionContext()

      val fa = handle(serviceA(client).process(TestRequestA(REQUEST_ID)))
      val fb = handle(serviceB(client).process(TestRequestB(REQUEST_ID)))

      Await.ready(fa zip fb, Duration.create(5, TimeUnit.SECONDS))
    } finally {
      client.stop()
      server.stop()
    }
  }

  private def startServer(ec: ExecutionContext): GrpcServer = {
    implicit val serverExecutionContext: ExecutionContext = ec
    val server = GrpcServer(
      HOSTNAME,
      PORT,
      ec => Seq(
        TestServiceAGrpc.bindService(new TestServiceAImpl(), ec),
        TestServiceBGrpc.bindService(new TestServiceBImpl(), ec))
    )
    server.start()
    server
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

  private def serviceA(client: GrpcClient) = client.createClient(channel => TestServiceAGrpc.stub(channel))

  private def serviceB(client: GrpcClient) = client.createClient(channel => TestServiceBGrpc.stub(channel))

  private def serverExecutorSvc() = {
    Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-server-%d").build())
  }

  private def clientExecutorSvc() =
    Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat("rpc-client-%d").build)
}
