package net.ndolgov.akkahttptest.web

import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, onComplete, path, pathPrefix, post, _}
import akka.http.scaladsl.server.Route
import net.ndolgov.akkahttptest.service.TestServiceC
import net.ndolgov.akkahttptest.web.HttpEndpoints._
import net.ndolgov.akkahttptest.web.ServiceCJsonMarshaller._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/** Streaming HTTP end point */
class HttpEndpointC(service: TestServiceC, requestTimeoutMs: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val prefix = pathPrefix("akkahttp" / "test")

  private val exceptionHandler = unexpectedExceptionHandler(logger)

  /** @return all Routes supported by this HTTP end point */
  def endpointRoutes(): Route = prefix {
    processPost ~ processGet
  }

  private def processPost: Route =
    handleExceptions(exceptionHandler) {
      post {
        path("testservicec") {
          parameters("key") { key =>
            extractDataBytes { stream =>
              onComplete(service.process(key, stream)) {
                case Success(response) =>
                  complete(response)

                case Failure(e) =>
                  completeWithError(e)
              }
            }
          }
        }
      }
    }

  private def processGet: Route =
    handleExceptions(exceptionHandler) {
      get {
        path("testservicec") {
          parameters("key") { key =>
              onComplete(service.process(key)) {
                case Success(stream) =>
                  complete(HttpResponse(
                    entity = HttpEntity(ContentType(MediaTypes.`application/octet-stream`), stream)
                  ))

                case Failure(e) =>
                  completeWithError(e)
              }
          }
        }
      }
    }

  private def completeWithError(e: Throwable) = {
    val message = "Unexpectedly failed to process request"
    logger.error(message, e)
    complete(httpErrorResponse(StatusCodes.InternalServerError, message))
  }
}
