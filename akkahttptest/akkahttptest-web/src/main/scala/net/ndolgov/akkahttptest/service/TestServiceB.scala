package net.ndolgov.akkahttptest.service

import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

case class TestRequestB(requestId: Long)

case class TestResponseB(success: Boolean, requestId: Long, result: String)

/** Service API analogous to the one generated from "testsvcB.proto" */
trait TestServiceB {
  def process(request: TestRequestB) : Future[TestResponseB]
}

final class TestServiceBImpl(implicit ec: ExecutionContext) extends TestServiceB {
  private val logger = LoggerFactory.getLogger(classOf[TestServiceBImpl])

  override def process(request: TestRequestB): Future[TestResponseB] = Future {
    logger.info("Computing result");  // todo this is where actual time-consuming processing would be
    TestResponseB(success = true, request.requestId, "RESULT")

  }
}