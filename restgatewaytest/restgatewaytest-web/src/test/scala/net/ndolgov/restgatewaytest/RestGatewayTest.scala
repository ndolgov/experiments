package net.ndolgov.restgatewaytest

import org.scalatest.{Assertions, FlatSpec}
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executor, ExecutorService, Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import net.ndolgov.restgatewaytest.api.testsvcA.{TestRequestA, TestServiceAGrpc, TestServiceAHandler}
import net.ndolgov.restgatewaytest.api.testsvcB.{TestRequestB, TestServiceBGrpc, TestServiceBHandler}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class TestResponseA1(success: Boolean, requestId: Long, result: String)
class TestResponseA2(success: Boolean, requestId: Long, result: String)

class RestGatewayTest extends FlatSpec with Assertions {
  private val logger = LoggerFactory.getLogger(classOf[RestGatewayTest])
  private val PORT = 20000
  private val GATEWAY_PORT = PORT + 1
  private val REQUEST_IDA = 123
  private val REQUEST_IDB = 124
  private val HOSTNAME = "127.0.0.1"
  private val THREADS = 4

  it should "support two service endpoints on the same port" in {
    val server = startServer(serverExecutorSvc())
    val gateway = startGateway(gatewayExecutorSvc())
    val client = GatewayClient(s"http://$HOSTNAME:$GATEWAY_PORT/restgateway/test", clientExecutorSvc())

    try {
      implicit val clientExecutionContext: ExecutionContext = client.executionContext()
      val fa = handle(client.callA(TestRequestA(REQUEST_IDA)))
      val fb = handle(client.callB(TestRequestB(REQUEST_IDB)))

      Await.ready(fa zip fb, Duration.create(5, TimeUnit.SECONDS))
    } finally {
      client.stop()
      gateway.stop()
      server.stop()
    }
  }

  private def startServer(executor: ExecutorService): GrpcServer = {
    implicit val serverExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
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

  private def startGateway(executor: Executor): GatewayServer = {
    implicit val serverExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val gateway = GatewayServer(
      HOSTNAME, PORT,
      GATEWAY_PORT,
      executor,
      channel => Seq(
        new TestServiceAHandler(channel),
        new TestServiceBHandler(channel))
    )
    gateway.start()
    gateway
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

  private def serverExecutorSvc(): ExecutorService = {
    executorSvc("rpc-server-%d")
  }

  private def gatewayExecutorSvc(): ExecutorService = {
    executorSvc("rpc-gateway-%d")
  }

  private def clientExecutorSvc(): ExecutorService =
    executorSvc("rpc-client-%d")

  private def executorSvc(format: String): ExecutorService =
    Executors.newFixedThreadPool(THREADS, new ThreadFactoryBuilder().setNameFormat(format).build)
}
