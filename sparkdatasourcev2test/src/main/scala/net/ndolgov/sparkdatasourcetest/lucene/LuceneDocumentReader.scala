package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.lucene.index.StoredFieldVisitor.Status
import org.apache.lucene.index.{FieldInfo, StoredFieldVisitor}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

/**
  * Collect fields from matching Lucene documents into Spark rows
  */
final class LuceneDocumentReader(val row : Array[Any], val fieldVisitor : StoredFieldVisitor) extends LuceneDocumentProcessor {
  private val rows = new ArrayBuffer[InternalRow](1024)

  /** @return Lucene field visitor to apply to all matching documents */
  override def visitor(): StoredFieldVisitor = fieldVisitor

  /** Process Lucene document fields gathered by the [[visitor]] */
  override def onDocument(): Unit = {
    rows += InternalRow.fromSeq(row.clone())

    // reset buffer; todo more efficient way?
    for (index <- row.indices) {
      row(index) = null
    }
  }

  def retrievedRows() : Array[InternalRow] = if (rows.isEmpty) Array() else rows.toArray
}

/**
  * Extract values of the requested fields from matching documents
  * @param needed the set of fields to extract
  * @param fieldToIndex mapping from a Lucene field name to the Spark Row column index
  * @param readers field readers arranged in the same order as used by fieldToIndex
  */
private final class LuceneFieldVisitor(val needed : Set[String], val fieldToIndex : Map[String, Int], val readers : Array[LuceneFieldReader]) extends StoredFieldVisitor {
  override def longField(fieldInfo : FieldInfo, value : Long) : Unit = {
    readers(fieldToIndex(fieldInfo.name)).readLong(value)
  }

  override def doubleField(fieldInfo : FieldInfo, value : Double) : Unit = {
    readers(fieldToIndex(fieldInfo.name)).readDouble(value)
  }

  override def needsField(fieldInfo : FieldInfo) : Status = {
    if(needed.contains(fieldInfo.name)) Status.YES else Status.NO
  }
}

// todo iterator
object LuceneDocumentReader {
  def apply(columns: Seq[String], filters: Array[Filter], schema: LuceneSchema) : LuceneDocumentReader = {
    val row: Array[Any] = new Array[Any](columns.length)
    val readers : Array[LuceneFieldReader] = LuceneFieldReader(columns, filters, schema, row)

    val fieldToIndex = new HashMap[String, Int]()
    var index : Int = 0
    for (column <- columns) {
      fieldToIndex.put(column, index)
      index += 1
    }

    new LuceneDocumentReader(row, new LuceneFieldVisitor(columns.toSet, fieldToIndex, readers))
  }
}