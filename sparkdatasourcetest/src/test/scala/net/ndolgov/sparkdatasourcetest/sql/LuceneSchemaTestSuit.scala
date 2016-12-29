package net.ndolgov.sparkdatasourcetest.sql

import java.io.File

import org.apache.spark.sql.types.StructType
import org.scalatest.{Assertions, FlatSpec}

final class LuceneSchemaTestSuit extends FlatSpec with Assertions {
  "A schema written to a file" should "be read back" in {
    val sparkSchema : StructType = LuceneDataSourceTestEnv.defaultSchema
    val luceneSchema : Array[FieldType] = Array[FieldType](FieldType.INDEXED, FieldType.QUERYABLE, FieldType.STORED)
    val original = LuceneSchema(sparkSchema, FieldType.toString(luceneSchema))

    val filePath: String = "target/testschema" + System.currentTimeMillis() + ".txt"
    LuceneSchema.save(original, filePath)

    val retrieved = LuceneSchema.open(filePath)

    assert(retrieved.size == 3)
    for (i <- 0 to 2) {
      assert(retrieved.sparkFieldType(i) == sparkSchema.fields(i).dataType)
      assert(retrieved.luceneFieldType(i) == luceneSchema(i))
    }

    new File(filePath).delete()
  }
}
