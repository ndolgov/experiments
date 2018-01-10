package net.ndolgov.akkahttptest

import java.util.concurrent.{ExecutorService, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{ActorMaterializer, Materializer}
import net.ndolgov.akkahttptest.service.{TestRequestA, TestRequestB, TestResponseA, TestResponseB}
import net.ndolgov.akkahttptest.web.{ServiceAJsonMarshaller, ServiceBJsonMarshaller}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

// https://stackoverflow.com/questions/41121193/akka-http-spray-json-client-side-json-marshalling
// https://groups.google.com/forum/#!msg/akka-user/F6gniCzdNyk/FDYaVj1fCAAJ

/** Test services HTTP client implemented with akka-http.
  * It relies on spray-json (triggered with explicit Marshal/Unmarshal calls) for JSON marshalling.  */
final class AkkaHttpClient(url: String, val executor: ExecutorService) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit val system: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val materializer: Materializer = ActorMaterializer()

  private val timeout = FiniteDuration.apply(10, TimeUnit.SECONDS)

  def callA(request: TestRequestA) : Future[TestResponseA] = {
    import ServiceAJsonMarshaller._

    post[TestRequestA,TestResponseA](
      request,
      "/testservicea",
      (req: TestRequestA) => Marshal(req).to[RequestEntity],
      (entity: HttpEntity.Strict) => Unmarshal(entity).to[TestResponseA])
  }

  def callB(request: TestRequestB) : Future[TestResponseB] = {
    import ServiceBJsonMarshaller._

    post[TestRequestB,TestResponseB](
      request,
      "/testserviceb",
      (req: TestRequestB) => Marshal(req).to[RequestEntity],
      (entity: HttpEntity.Strict) => Unmarshal(entity).to[TestResponseB])
  }

  /**
    * Make a POST request to a given service and return the received response
    * @param request request message
    * @param path service-specific URL path suffix
    * @param fromRequest request message-to-JSON marshaller
    * @param toResponse JSON-to-response message unmarshaller
    * @tparam REQUEST request message type
    * @tparam RESPONSE response message type
    * @return unmarshalled response message wrapped into a Future
    */
  private def post[REQUEST, RESPONSE](
    request: REQUEST,
    path: String,
    fromRequest: REQUEST => Future[RequestEntity],
    toResponse: HttpEntity.Strict => Future[RESPONSE]) : Future[RESPONSE] = {

    logger.info(s"Sending $request")

    for {
      body <- fromRequest(request)
      httpResponse <- Http().singleRequest(HttpRequest(uri = url + path, entity = body, method = POST))
      entity <- httpResponse.entity.toStrict(timeout)
      response <- toResponse(entity)
    } yield response
  }
}
