package net.ndolgov.restgatewaytest

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/** gRPC service instance that can be started and stopped */
trait GrpcServer {
  def start(): Unit

  def stop(timeout: Long): Unit
}

private final class GrpcServerImpl(server: Server, port: Int) extends GrpcServer {
  private val logger = LoggerFactory.getLogger(classOf[GrpcServer])

  override def start(): Unit = {
    try {
      logger.info("Starting " + this)
      server.start
      logger.info("Started " + this)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Could not start server", e)
    }
  }

  override def stop(timeout: Long): Unit = {
    try {
      logger.info("Stopping " + this)
      server.shutdown().awaitTermination(timeout, TimeUnit.MILLISECONDS)
      logger.info("Stopped " + this)
    } catch {
      case _: Exception =>
        logger.warn("Interrupted while shutting down " + this)
    }
  }

  override def toString: String = "{GrpcServer:port=" + port + "}"
}

/** Create a Netty-backed gRPC service instance with the request handlers created by a given factory method.
  * Bind the service to a given "host:port" address. Process requests on a given thread pool. */
object GrpcServer {
  def apply(hostname: String,
            port: Int,
            toServices: (ExecutionContext) => Seq[ServerServiceDefinition])
           (implicit ec: ExecutionContext) : GrpcServer = {

    val builder: NettyServerBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(hostname, port))
    toServices.apply(ec).foreach(definition => builder.addService(definition))

    new GrpcServerImpl(builder.build(), port)
  }
}
