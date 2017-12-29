package net.ndolgov.scalapbtest

import net.ndolgov.scalapbtest.api.testsvcA.{TestRequestA, TestResponseA}
import net.ndolgov.scalapbtest.api.testsvcA.TestServiceAGrpc.TestServiceA
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class TestServiceAImpl(implicit ec: ExecutionContext) extends TestServiceA {
  private val logger = LoggerFactory.getLogger(classOf[TestServiceAImpl])

  override def process(request: TestRequestA): Future[TestResponseA] = {
    Future {
      logger.info("Computing result");  // todo this is where actual time-consuming processing would be
      TestResponseA(success = true, request.requestId, "RESULT")
    }
  }
}
