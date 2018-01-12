package net.ndolgov.restgatewaytest

import java.util.concurrent.ExecutorService

import net.ndolgov.restgatewaytest.api.testsvcA.{TestRequestA, TestResponseA}
import net.ndolgov.restgatewaytest.api.testsvcB.{TestRequestB, TestResponseB}
import org.apache.http.HttpResponse
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import JsonMarshaller._

/** This class represents the idea of "an HTTP client that can make a POST request with JSON body attached".
  * For simplicity it's actually implemented with Apache httpclient library. Jackson JSON marshaller is used for message
  * (de)serialization.
  * Both could be compared to Akka HTTP-based client and Spray-based marshaller in [[https://github.com/ndolgov/experiments/tree/master/akkahttptest akkahttptest]]. */
final class GatewayClient(client: CloseableHttpClient, url: String)(implicit val ec: ExecutionContext)  {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def stop(): Unit = {
    client.close()
  }

  def executionContext() : ExecutionContext = ec

  def callA(request: TestRequestA) : Future[TestResponseA] = {
    post(request, "/testservicea", classOf[TestResponseA])
  }
  def callB(request: TestRequestB) : Future[TestResponseB] = {
    post(request, "/testserviceb", classOf[TestResponseB])
  }

  private def post[REQUEST, RESPONSE](request: REQUEST, path: String, clazz: Class[RESPONSE]) : Future[RESPONSE] = {
    Future {
      logger.info(s"Sending $request")
      val httpRequest: HttpPost = new HttpPost(url + path)
      httpRequest.setEntity(new StringEntity(toJson(request)))
      client.execute(httpRequest, handler(clazz))
    }
  }

  private def handler[RESPONSE](clazz: Class[RESPONSE]): ResponseHandler[RESPONSE] = {
    (httpResponse: HttpResponse) => {
      val status = httpResponse.getStatusLine.getStatusCode
      logger.info(s"Handling response with status $status")

      if (status < 200 && status >= 300) {
        throw new RuntimeException("Unexpected response status: " + status)
      }

      val entity = httpResponse.getEntity
      if (entity == null) {
        throw new RuntimeException("No response body found")
      }

      fromJson(EntityUtils.toString(entity), clazz)
    }
  }
}

/** Create HTTP client to a given service URL */
object GatewayClient {
  def apply(url: String, executor: ExecutorService) : GatewayClient = {
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    new GatewayClient(HttpClients.createDefault(), url)
  }
}
