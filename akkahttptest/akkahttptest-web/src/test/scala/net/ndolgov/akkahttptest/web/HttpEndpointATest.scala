package net.ndolgov.akkahttptest.web

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{MediaTypes, RequestEntity, StatusCode, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import net.ndolgov.akkahttptest.service.{TestRequestA, TestResponseA, TestServiceA, TestServiceAImpl}
import org.mockito.{ArgumentMatchers => AM}
import org.mockito.Mockito.{verify, when}
import org.scalatest.{Assertions, FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import ServiceAJsonMarshaller._
import org.scalatest.mockito.MockitoSugar

class HttpEndpointATest extends FlatSpec with Assertions with Matchers with MockitoSugar with ScalatestRouteTest {
  private val REQUEST_ID = 123

  private val SERVICEA_PATH = "/akkahttp/test/testservicea"

  it should "process a valid request successfully" in {
    val route = new HttpEndpointA(new TestServiceAImpl(), 10000).endpointRoutes()
    val request: RequestEntity = Await.result(Marshal(TestRequestA(REQUEST_ID)).to[RequestEntity], Duration.Inf)

    Post(SERVICEA_PATH).withEntity(request) ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[TestResponseA] shouldEqual TestResponseA(true, REQUEST_ID, "RESULTA")
      contentType.mediaType shouldEqual MediaTypes.`application/json`
    }
  }

  it should "detect a garbled request" in {
    val route = new HttpEndpointA(new TestServiceAImpl(), 10000).endpointRoutes()

    Post(SERVICEA_PATH).withEntity("{\"garbled\":\"request\"})") ~> route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldEqual "Could not parse request"
      contentType.mediaType shouldEqual MediaTypes.`application/json`
    }
  }

  it should "cope with failed request processing" in {
    val service = mock[TestServiceA]
    when(service.process(AM.any[TestRequestA])).thenReturn(Future.failed(new RuntimeException("Simulating failed request processing")))

    val route = new HttpEndpointA(service, 10000).endpointRoutes()
    val request: RequestEntity = Await.result(Marshal(TestRequestA(REQUEST_ID)).to[RequestEntity], Duration.Inf)

    Post(SERVICEA_PATH).withEntity(request) ~> route ~> check {
      status shouldEqual StatusCodes.InternalServerError
      responseAs[String] shouldEqual "Unexpectedly failed to process request"
      contentType.mediaType shouldEqual MediaTypes.`application/json`
    }
  }

  it should "survive uncaught exception" in {
    val service = mock[TestServiceA]
    when(service.process(AM.any[TestRequestA])).thenThrow(new RuntimeException("Simulating uncaught exception"))

    val route = new HttpEndpointA(service, 10000).endpointRoutes()
    val request: RequestEntity = Await.result(Marshal(TestRequestA(REQUEST_ID)).to[RequestEntity], Duration.Inf)

    Post(SERVICEA_PATH).withEntity(request) ~> route ~> check {
      status shouldEqual StatusCodes.InternalServerError
      responseAs[String] shouldEqual "Unexpected error: Simulating uncaught exception"
      contentType.mediaType shouldEqual MediaTypes.`application/json`
    }
  }
}
