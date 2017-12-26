package net.ndolgov.sparkdatasourcetest.connector

import java.io.File

import net.ndolgov.sparkdatasourcetest.connector.LuceneDataSourceTestEnv.DocumentField
import net.ndolgov.sparkdatasourcetest.lucene.{FieldType, LuceneField, LuceneSchema}
import org.apache.spark.sql.types.StructType
import org.scalatest.{Assertions, FlatSpec}

final class LuceneSchemaTestSuit extends FlatSpec with Assertions {
  "A schema written to a file" should "be read back" in {
    val METRIC = DocumentField.METRIC.name
    val TIME = DocumentField.TIME.name
    val VALUE = DocumentField.VALUE.name

    val sparkSchema : StructType = LuceneDataSourceTestEnv.defaultSchema

    val luceneSchema : Array[LuceneField] = Array[LuceneField](
      LuceneField(METRIC, FieldType.QUERYABLE),
      LuceneField(TIME, FieldType.INDEXED),
      LuceneField(VALUE, FieldType.STORED))

    val original = LuceneSchema(sparkSchema, LuceneField.toString(luceneSchema))

    val filePath: String = "target/testschema" + System.currentTimeMillis() + ".txt"
    LuceneSchema.save(original, filePath)

    val retrieved = LuceneSchema.open(filePath)

    assert(retrieved.size == 3)
    for (i <- 0 to 2) {
      assert(retrieved.sparkField(i) == sparkSchema.fields(i))
      assert(retrieved.luceneField(i) == luceneSchema(i))
    }

    new File(filePath).delete()
  }
}
