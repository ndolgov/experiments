package net.ndolgov.restgatewaytest

import net.ndolgov.restgatewaytest.api.testsvcB.{TestRequestB, TestResponseB}
import net.ndolgov.restgatewaytest.api.testsvcB.TestServiceBGrpc.TestServiceB
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class TestServiceBImpl(implicit ec: ExecutionContext) extends TestServiceB {
  private val logger = LoggerFactory.getLogger(classOf[TestServiceBImpl])

  override def process(request: TestRequestB): Future[TestResponseB] = {
    Future {
      logger.info(s"Computing result of $request");  // todo this is where actual time-consuming processing would be
      TestResponseB(success = true, request.requestId, "RESULTB")
    }
  }
}
