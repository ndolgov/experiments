package net.ndolgov.akkahttptest.saga

import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

/** An object id with a revision assigned by the storage */
case class ObjectId(name: String, revision: Option[String])

/** In real life files would be in HDFS */
trait ObjectStorage {
  def createFile(obj: Array[Byte], path: String): Try[Unit]

  def createFile(obj: Source[ByteString, Any], path: String): Future[Long]

  def renameFile(from: String, to: String): Try[Unit]

  def deleteFile(path: String): Option[Try[Unit]]

  def tmpPath(objId: ObjectId): String

  def persistentPath(objId: ObjectId): String
}

/** In real life it would invoke a DAO to modify the DB */
trait ObjectCatalog {
  def createRevision(objId: ObjectId): Try[String]

  def forgetRevision(objId: ObjectId): Try[Unit]
}
