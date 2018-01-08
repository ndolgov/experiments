package net.ndolgov.akkahttptest.web

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directives.{as, complete, entity, onComplete, path, pathPrefix, post, withRequestTimeout}
import akka.http.scaladsl.server.Route
import net.ndolgov.akkahttptest.service.{TestRequestA, TestServiceA}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

import HttpEndpoints._
import ServiceAJsonMarshaller._

class HttpEndpointA(service: TestServiceA, requestTimeoutMs: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val prefix = pathPrefix("akkahttp" / "test")

  private val maxRequestDuration = Duration.create(requestTimeoutMs, TimeUnit.MILLISECONDS)

  private val exceptionHandler = unexpectedExceptionHandler(logger)

  private val rejectionHandler = garbledRequestHandler(logger)

  def endpointRoutes(): Route = prefix {
    process
  }

  private def process: Route =
    handleExceptions(exceptionHandler) {
      post {
        path("testservicea") {
          handleRejections(rejectionHandler) {
            entity(as[TestRequestA]) { request: TestRequestA =>
              withRequestTimeout(maxRequestDuration)
              onComplete(service.process(request)) {
                case Success(response) =>
                  complete(response)

                case Failure(e) =>
                  val message = "Unexpectedly failed to process request"
                  logger.error(message, e)
                  complete(httpErrorResponse(StatusCodes.InternalServerError, message))
              }
            }
          }
        }
      }
    }
}
