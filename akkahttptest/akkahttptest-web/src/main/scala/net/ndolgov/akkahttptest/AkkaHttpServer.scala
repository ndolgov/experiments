package net.ndolgov.akkahttptest

import java.util.concurrent.ExecutorService

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

// https://github.com/RayRoestenburg/akka-in-action/blob/master/chapter-integration/src/main/scala/aia/integration/OrderServiceApp.scala

trait AkkaHttpServer {
  def start(route: ExecutionContext => Route): Future[AkkaHttpServer]

  def stop(): Future[AkkaHttpServer]
}

private case object FailedServer extends AkkaHttpServer {
  override def start(route: ExecutionContext => Route): Future[AkkaHttpServer] = Future.failed(new RuntimeException("Already tried"))

  override def stop(): Future[AkkaHttpServer] = Future.successful(this)
}

private final class InitializedServer(host: String, port: Int)
  (implicit val system: ActorSystem, implicit val ec: ExecutionContext, implicit val materializer: Materializer) extends AkkaHttpServer {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def start(route: ExecutionContext => Route): Future[AkkaHttpServer] = {
    log.info(s"Starting $this")

    Http().
      bindAndHandle(route(ec), host, port).
      map((binding: ServerBinding) => {
        StartedServer(binding)
      }).
      recoverWith({ case e: Exception =>
        log.error(s"Failed to bind to $host:$port", e)
        stop()
      })
  }

  override def stop(): Future[AkkaHttpServer] = {
    system.terminate().flatMap(_ => FailedServer.stop())
  }

  override def toString: String = s"{AkkaHttpServer:port=$port}"

  private case class StartedServer(binding: ServerBinding) extends AkkaHttpServer {
    log.info(s"Bound to ${binding.localAddress} ")

    override def start(route: ExecutionContext => Route): Future[AkkaHttpServer] = Future.failed(new RuntimeException("Already started"))

    override def stop(): Future[AkkaHttpServer] = {
      log.info(s"Stopping $this")
      binding.unbind().flatMap(_ => system.terminate()).map(_ => this)
    }
  }
}

object AkkaHttpServer {
  def apply(host: String, port: Int, executor: Option[ExecutorService]) : AkkaHttpServer = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = executor.
      map((es: ExecutorService) => ExecutionContext.fromExecutor(es)).
      getOrElse(system.dispatcher)

    new InitializedServer(host, port)
  }

  def apply(): AkkaHttpServer = {
    val config = ConfigFactory.load()

    apply(
      config.getString("http.host"),
      config.getInt("http.port"),
      None)
  }
}


