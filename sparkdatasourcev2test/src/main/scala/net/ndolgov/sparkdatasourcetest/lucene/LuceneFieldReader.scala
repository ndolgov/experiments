package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DoubleType, LongType, StructField}

/**
  * Read a field value from the current Lucene document to the current Spark Row
  */
trait LuceneFieldReader {
  def readLong(value : Long) : Unit = throw new UnsupportedOperationException

  def readDouble(value : Double) : Unit = throw new UnsupportedOperationException
}

object LuceneFieldReader {

  // todo retrieve fields for (filters - columns) attrs?
  def apply(columns: Seq[String], filters: Array[Filter], schema: LuceneSchema, row : Array[Any]) : Array[LuceneFieldReader] = {
    val readers : Array[LuceneFieldReader] = Array.ofDim[LuceneFieldReader](columns.length)

    var schemaIndex : Int = 0
    var outputIndex : Int = 0
    for (field <- schema.sparkSchema()) {
      if (columns.contains(field.name)) {
        readers(outputIndex) = apply(field, outputIndex, row)
        outputIndex += 1
      }
      schemaIndex += 1
    }

    readers
  }

  private def apply(sparkField: StructField, outputIndex : Int, row: Array[Any]): LuceneFieldReader = {
    sparkField.dataType match {
      case LongType => new LongReader(outputIndex, row)
      case DoubleType => new DoubleReader(outputIndex, row)
      case _ => throw new IllegalArgumentException("Unsupported field type: " + sparkField.dataType);
    }
  }

  private final class LongReader(index : Int, row: Array[Any]) extends LuceneFieldReader {
    override def readLong(value : Long) : Unit = {
      row(index) = value
    }
  }

  private final class DoubleReader(index : Int, row: Array[Any]) extends LuceneFieldReader {
    override def readDouble(value : Double) : Unit = {
      row(index) = value
    }
  }
}
