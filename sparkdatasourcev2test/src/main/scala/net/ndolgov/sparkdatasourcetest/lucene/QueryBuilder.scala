package net.ndolgov.sparkdatasourcetest.lucene

import org.apache.lucene.document.LongPoint
import org.apache.lucene.search.{BooleanClause, BooleanQuery, MatchAllDocsQuery, Query}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, LessThan}
import org.apache.spark.sql.types.{LongType, StructField}

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
  * Currently supported: "==", "<", ">" on indexed fields of the long type.
  */
object QueryBuilder {
  def apply(filters: Array[Filter], schema : LuceneSchema) : Query = {
    if (filters.isEmpty) {
      return new MatchAllDocsQuery()
    }

    val builder = new QueryBuilder()

    filters.foreach {
      case EqualTo(attr, value) =>
        val sparkField: StructField = schema.sparkField(assertFieldIsIndexed(schema, attr))
        val query = sparkField.dataType match {
          case LongType => LongPoint.newExactQuery(attr, value.asInstanceOf[Long])
          case _ => throw new IllegalArgumentException("Unsupported field: " + attr + " of type: " + sparkField.dataType );
        }

        builder.must(query)

      case GreaterThan(attr, value) =>
        val sparkField: StructField = schema.sparkField(assertFieldIsIndexed(schema, attr))
        val query = sparkField.dataType match {
          case LongType => LongPoint.newRangeQuery(attr, value.asInstanceOf[Long], Long.MaxValue)
          case _ => throw new IllegalArgumentException("Unsupported field: " + attr + " of type: " + sparkField.dataType );
        }

        builder.must(query)

      case LessThan(attr, value) =>
        val sparkField: StructField = schema.sparkField(assertFieldIsIndexed(schema, attr))
        val query = sparkField.dataType match {
          case LongType => LongPoint.newRangeQuery(attr, Long.MinValue,  value.asInstanceOf[Long])
          case _ => throw new IllegalArgumentException("Unsupported field: " + attr + " of type: " + sparkField.dataType );
        }

        builder.must(query)

      case filter : Filter => throw new IllegalArgumentException("Unsupported filter type: " + filter)
    }

    builder.build()
  }

  private def assertFieldIsIndexed(schema: LuceneSchema, attr: String): Int = {
    val index = schema.sparkSchema().fieldIndex(attr)

    if (schema.luceneFieldType(index) == FieldType.STORED) {
      throw new IllegalArgumentException("Field is not indexed: " + attr)
    }

    index
  }
}
