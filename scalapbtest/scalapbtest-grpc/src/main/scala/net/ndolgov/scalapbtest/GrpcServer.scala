package net.ndolgov.scalapbtest

import java.net.InetSocketAddress

import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ServerServiceDefinition}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext

trait GrpcServer {
  def start(): Unit

  def stop(): Unit
}

private final class GrpcServerImpl(server: Server, port: Int) extends GrpcServer {
  private val logger = LoggerFactory.getLogger(classOf[GrpcServer])

  override def start(): Unit = {
    try {
      server.start
      logger.info("Started " + this)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Could not start server", e)
    }
  }

  override def stop(): Unit = {
    try {
      logger.info("Stopping " + this)
      server.shutdown
      logger.info("Stopped " + this)
    } catch {
      case e: Exception =>
        logger.warn("Interrupted while shutting down " + this)
    }
  }

  override def toString: String = "{GrpcServer:port=" + port + "}"
}

/** Create a GRPC server for given request handlers and bind it to provided "host:port" */
object GrpcServer {
  def apply(hostname: String, port: Int, toServices: (ExecutionContext) => Seq[ServerServiceDefinition])
           (implicit ec: ExecutionContext) : GrpcServer = {
    val builder: NettyServerBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(hostname, port))
    toServices.apply(ec).foreach(definition => builder.addService(definition))

    new GrpcServerImpl(builder.build(), port)
  }
}
