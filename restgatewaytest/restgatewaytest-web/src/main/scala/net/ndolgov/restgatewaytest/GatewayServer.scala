package net.ndolgov.restgatewaytest

import java.util.concurrent.Executor

import grpcgateway.handlers.GrpcGatewayHandler
import grpcgateway.server.{GrpcGatewayServer, GrpcGatewayServerBuilder}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.slf4j.LoggerFactory

/** REST gateway for a gRPC service instance that can be started and stopped */
trait GatewayServer {
  def start(): Unit

  def stop(): Unit
}

private final class GatewayServerImpl(server: GrpcGatewayServer, port: Int) extends GatewayServer {
  private val logger = LoggerFactory.getLogger(classOf[GatewayServer])

  override def start(): Unit = {
    try {
      server.start()
      logger.info("Started " + this)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Could not start server", e)
    }
  }

  override def stop(): Unit = {
    try {
      logger.info("Stopping " + this)
      server.shutdown()
      logger.info("Stopped " + this)
    } catch {
      case _: Exception =>
        logger.warn("Interrupted while shutting down " + this)
    }
  }

  override def toString: String = "{GatewayServer:port=" + port + "}"
}

/** Create a Netty-backed REST Gateway for a given gRPC server with the request handlers created by a given factory
  * method. Bind the gateway to a given port. Perform request redirection on a given thread pool. */
object GatewayServer {
  def apply(serviceHost: String, servicePort: Int,
            gatewayPort: Int,
            executor: Executor,
            toHandlers: (ManagedChannel) => Seq[GrpcGatewayHandler]) : GatewayServer = {
    val channel = ManagedChannelBuilder.forAddress(serviceHost, servicePort).usePlaintext(true).executor(executor).build()

    var builder = GrpcGatewayServerBuilder.forPort(gatewayPort)
    for (handler <- toHandlers(channel)) {
      builder = builder.addService(handler)
    }

    new GatewayServerImpl(builder.build(), gatewayPort)
  }
}
