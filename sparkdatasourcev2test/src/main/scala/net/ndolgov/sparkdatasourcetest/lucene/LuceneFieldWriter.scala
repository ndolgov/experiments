package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.lucene.document.Document
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType}

/**
  * Write a Spark Row to a Lucene index as a document
  */
trait LuceneFieldWriter {
  def write(row : Row)
}

private final class RowWriter(writers : Seq[LuceneFieldWriter], document : Document) extends LuceneFieldWriter {
  override def write(row: Row): Unit = {
    writers.foreach((writer: LuceneFieldWriter) => writer.write(row))
  }
}

private final class LongFieldWriter(index: Int, field: LuceneDocumentField, document : Document) extends LuceneFieldWriter {
  override def write(row: Row): Unit = {
    if (!row.isNullAt(index)) {
      field.addTo(document)
      field.setLongValue(row.getLong(index))
    }
  }
}

private final class DoubleFieldWriter(index: Int, field: LuceneDocumentField, document: Document) extends LuceneFieldWriter {
  override def write(row: Row): Unit = {
    if (!row.isNullAt(index)) {
      field.addTo(document)
      field.setDoubleValue(row.getDouble(index))
    }
  }
}

object LuceneFieldWriter {
  /**
    * @param schema document schema
    * @param document document instance to reuse
    * @return a new writer
    */
  def apply(schema : LuceneSchema, document : Document) : LuceneFieldWriter = {
    val writers : Array[LuceneFieldWriter] = Array.ofDim[LuceneFieldWriter](schema.size)

    var index : Int = 0
    for (_ <- schema.sparkSchema()) {
      writers(index) = apply(schema, index, document)
      index += 1
    }

    new RowWriter(writers, document)
  }

  private def apply(schema : LuceneSchema, index : Int, document : Document) : LuceneFieldWriter = {
    val field : LuceneDocumentField = LuceneFieldFactory(schema, index)

    schema.sparkFieldType(index) match {
      case LongType => new LongFieldWriter(index, field, document)
      case DoubleType => new DoubleFieldWriter(index, field, document)
      case _ => throw new IllegalArgumentException("Unsupported field type: " + field);
    }
  }
}
