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

final class GatewayClient(client: CloseableHttpClient, url: String)(implicit val ec: ExecutionContext)  {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val responseAClass = TestResponseA().getClass.asInstanceOf[Class[TestResponseA]]

  private val responseBClass = TestResponseB().getClass.asInstanceOf[Class[TestResponseB]]

  def stop(): Unit = {
    client.close()
  }

  def executionContext() : ExecutionContext = ec

  def callA(request: TestRequestA) : Future[TestResponseA] = {
    post(request, "/testservicea", responseAClass)
  }
  def callB(request: TestRequestB) : Future[TestResponseB] = {
    post(request, "/testserviceb", responseBClass)
  }

  private def post[REQUEST, RESPONSE](request: REQUEST, path: String, clazz: Class[RESPONSE]) : Future[RESPONSE] = {
    Future {
      logger.info(s"Sending $request")
      val httpRequest: HttpPost = new HttpPost(url + path)
      httpRequest.setEntity(new StringEntity(toJson(request)))
      client.execute(httpRequest, handler[RESPONSE](clazz))
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

      fromJson[RESPONSE](EntityUtils.toString(entity), clazz)
    }
  }
}

/** Create GRPC transport to a given "host:port" destination */
object GatewayClient {
  def apply(url: String, executor: ExecutorService) : GatewayClient = {
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    new GatewayClient(HttpClients.createDefault(), url)
  }
}
