package net.ndolgov.akkahttptest.web

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import net.ndolgov.akkahttptest.service.{TestRequestA, TestRequestB, TestResponseA, TestResponseB}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

// akka-http directives such as "as" and "complete" can transparently perform JSON (de)serialization as long as
// an appropriate RootJsonFormat is implicitly available
// for details, see https://doc.akka.io/docs/akka-http/current/common/json-support.html

/** spray-json ServiceA message marshaller */
object ServiceAJsonMarshaller extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val requestAMarshaller: RootJsonFormat[TestRequestA] = jsonFormat1(TestRequestA)
  implicit val responseAMarshaller: RootJsonFormat[TestResponseA] = jsonFormat3(TestResponseA)
}

/** spray-json ServiceB message marshaller */
object ServiceBJsonMarshaller extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val requestBMarshaller: RootJsonFormat[TestRequestB] = jsonFormat1(TestRequestB)
  implicit val responseBMarshaller: RootJsonFormat[TestResponseB] = jsonFormat3(TestResponseB)
}
