package net.ndolgov.restgatewaytest

import java.util.concurrent.Executor

import grpcgateway.handlers.GrpcGatewayHandler
import grpcgateway.server.{GrpcGatewayServer, GrpcGatewayServerBuilder}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.slf4j.LoggerFactory

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

/** Create a REST Gateway for a given GRPC server with request handlers and bind it to provided "host:port" */
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
