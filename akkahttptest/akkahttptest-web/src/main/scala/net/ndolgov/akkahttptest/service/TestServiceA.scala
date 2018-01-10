package net.ndolgov.akkahttptest.service

import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

case class TestRequestA(requestId: Long)

case class TestResponseA(success: Boolean, requestId: Long, result: String)

/** Service API analogous to the one generated from "testsvcA.proto" */
trait TestServiceA {
  def process(request: TestRequestA) : Future[TestResponseA]
}

final class TestServiceAImpl(implicit ec: ExecutionContext) extends TestServiceA {
  private val logger = LoggerFactory.getLogger(classOf[TestServiceAImpl])

  override def process(request: TestRequestA): Future[TestResponseA] = Future {
    logger.info("Computing result");  // todo this is where actual time-consuming processing would be
    TestResponseA(success = true, request.requestId, "RESULT")
  }
}