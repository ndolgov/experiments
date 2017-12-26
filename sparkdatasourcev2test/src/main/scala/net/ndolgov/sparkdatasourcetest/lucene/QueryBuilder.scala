package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.lucene.document.LongPoint
import org.apache.lucene.search.{BooleanClause, BooleanQuery, MatchAllDocsQuery, Query}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.{LongType, StructField}

import scala.collection.mutable.ArrayBuffer

/**
  * Compile a set of Spark filters into a Lucene query
  */
final class QueryBuilder {
  private val composite : BooleanQuery.Builder = new BooleanQuery.Builder()

  def must(query: Query) {
    composite.add(query, BooleanClause.Occur.MUST)
  }

  def mustNot(query: Query) {
    composite.add(query, BooleanClause.Occur.MUST_NOT)
  }

  def build(): BooleanQuery = {
    composite.build()
  }
}

/**
  * Currently supported: "==", "<", ">", "<=", ">=" on indexed fields of the long type.
  */
object QueryBuilder {
  def apply(filters: Array[Filter], schema : LuceneSchema) : Query = {
    if (filters.isEmpty) {
      return new MatchAllDocsQuery()
    }

    val builder = new QueryBuilder()

    filters.foreach {
      case EqualTo(attr, value) =>
        val sparkField: StructField = indexedField(schema, attr)
        val query = sparkField.dataType match {
          case LongType => LongPoint.newExactQuery(attr, value.asInstanceOf[Long])
          case _ => throw new IllegalArgumentException("Unsupported field: " + attr + " of type: " + sparkField.dataType );
        }

        builder.must(query)

      case GreaterThan(attr, value) =>
        builder.must(longRangeQuery(indexedField(schema, attr), value.asInstanceOf[Long] + 1, Long.MaxValue))

      case GreaterThanOrEqual(attr, value) =>
        builder.must(longRangeQuery(indexedField(schema, attr), value.asInstanceOf[Long], Long.MaxValue))

      case LessThan(attr, value) =>
        builder.must(longRangeQuery(indexedField(schema, attr), Long.MinValue,  value.asInstanceOf[Long] - 1))

      case LessThanOrEqual(attr, value) =>
        builder.must(longRangeQuery(indexedField(schema, attr), Long.MinValue,  value.asInstanceOf[Long]))

      case filter : Filter => throw new IllegalArgumentException("Unsupported filter type: " + filter)
    }

    builder.build()
  }

  private def indexedField(schema: LuceneSchema, attr: String): StructField = {
    schema.sparkField(assertFieldIsIndexed(schema, attr))
  }

  private def longRangeQuery(sparkField: StructField, from: Long, to: Long): Query = {
    sparkField.dataType match {
      case LongType => LongPoint.newRangeQuery(sparkField.name, from, to)
      case _ => throw new IllegalArgumentException("Unsupported field: " + sparkField);
    }
  }

  def distinguishSupported(filters: Array[Filter]) : (Array[Filter], Array[Filter]) = {
    val supported = new ArrayBuffer[Filter](filters.length)
    val unsupported = new ArrayBuffer[Filter](filters.length)

    filters.foreach {
      case et: EqualTo => supported += et
      case gt: GreaterThan => supported += gt
      case gte: GreaterThanOrEqual => supported += gte
      case lt: LessThan => supported += lt
      case lte: LessThanOrEqual => supported += lte
      case filter: Filter => unsupported += filter
    }

    (supported.toArray, unsupported.toArray)
  }

  private def assertFieldIsIndexed(schema: LuceneSchema, attr: String): String = {
    if (schema.luceneField(attr).fieldType == FieldType.STORED) {
      throw new IllegalArgumentException("Field is not indexed: " + attr)
    }

    attr
  }
}
