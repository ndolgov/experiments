package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.lucene.document.{Document, DoublePoint, LongPoint, StoredField}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField}

/**
  * A Lucene field that can be reused when writing multiple Lucene documents
  */
trait LuceneDocumentField {
  def addTo(document : Document) : Unit = throw new UnsupportedOperationException

  def setLongValue(value: Long) : Unit = throw new UnsupportedOperationException

  def setDoubleValue(value: Double) : Unit = throw new UnsupportedOperationException
}

/**
  * Create Lucene fields corresponding to a given Spark schema
  */
object LuceneFieldFactory {
  private val NO_VALUE_LONG: Long = -1L

  private val NO_VALUE_DOUBLE: Double = -1D

  def apply(schema : LuceneSchema, index : Int) : LuceneDocumentField = {
    val sparkField: StructField = schema.sparkField(index)

    // todo support NumericDocValues
    sparkField.dataType match {
      case LongType => schema.luceneFieldType(index) match {
        case FieldType.INDEXED => new IndexedLongField(new LongPoint(sparkField.name, NO_VALUE_LONG))
          
        case FieldType.STORED => new StoredLongField(new StoredField(sparkField.name, NO_VALUE_LONG))
        
        case FieldType.QUERYABLE => new QueryableLongField(
          new LongPoint(sparkField.name, NO_VALUE_LONG),
          new StoredField(sparkField.name, NO_VALUE_LONG))
      }
        
      case DoubleType => schema.luceneFieldType(index) match {
        case FieldType.INDEXED => new IndexedDoubleField(new DoublePoint(sparkField.name, NO_VALUE_DOUBLE))

        case FieldType.STORED => new StoredDoubleField(new StoredField(sparkField.name, NO_VALUE_DOUBLE))

        case FieldType.QUERYABLE => new QueryableDoubleField(
          new DoublePoint(sparkField.name, NO_VALUE_DOUBLE),
          new StoredField(sparkField.name, NO_VALUE_DOUBLE))
      }
      
      case _ => throw new IllegalArgumentException("Unsupported field: " + sparkField.name + " of type: " + sparkField.dataType);
    }
  }

  private final class QueryableLongField(indexed : LongPoint, stored : StoredField) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = {
      document.add(indexed)
      document.add(stored)
    }

    override def setLongValue(value: Long): Unit = {
      indexed.setLongValue(value)
      stored.setLongValue(value)
    }
  }

  private final class IndexedLongField(indexed : LongPoint) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = document.add(indexed)

    override def setLongValue(value: Long): Unit = indexed.setLongValue(value)
  }

  private final class StoredLongField(stored : StoredField) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = document.add(stored)

    override def setLongValue(value: Long): Unit = stored.setLongValue(value)
  }

  private final class QueryableDoubleField(indexed : DoublePoint, stored : StoredField) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = {
      document.add(indexed)
      document.add(stored)
    }

    override def setDoubleValue(value: Double): Unit = {
      indexed.setDoubleValue(value)
      stored.setDoubleValue(value)
    }
  }

  private final class IndexedDoubleField(indexed : DoublePoint) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = document.add(indexed)

    override def setDoubleValue(value: Double): Unit = indexed.setDoubleValue(value)
  }

  private final class StoredDoubleField(stored : StoredField) extends LuceneDocumentField {
    override def addTo(document: Document): Unit = document.add(stored)

    override def setDoubleValue(value: Double): Unit = stored.setDoubleValue(value)
  }
}