package net.ndolgov.akkahttptest.saga

import scala.collection.mutable

/** The state shared among the transactions of a saga */
trait TxContext {
  def getString(name: String): String

  def getLong(name: String): Long

  def setString(name: String, value: String): Unit

  def setLong(name: String, value: Long): Unit
}

private final class TxContextImpl extends TxContext {
  private val kvs = mutable.Map[String, AnyVal]()

  override def getString(name: String): String = kvs(name).asInstanceOf[String]

  override def getLong(name: String): Long = kvs(name).asInstanceOf[Long]

  override def setString(name: String, value: String): Unit = kvs.put(name, value.asInstanceOf[AnyVal])

  override def setLong(name: String, value: Long): Unit = kvs.put(name, value)
}

object TxContext {
  val TmpLocation = "TmpLocation"

  val PermanentLocation = "PermanentLocation"

  val Size = "Size"

  val Revision = "Revision"

  def apply(): TxContext = new TxContextImpl
}


