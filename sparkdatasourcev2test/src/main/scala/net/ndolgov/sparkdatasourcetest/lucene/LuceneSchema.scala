package net.ndolgov.sparkdatasourcetest.lucene

import java.io.FileWriter

import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable
import scala.io.Source

/**
  * The data schema used by the connector and comprised of a Spark schema (fields types) and a Lucene schema
  * (can a field be retrieved or filtered on?). The latter is necessary because not all Lucene documents are required
  * to have all the fields and so reverse engineering the schema from a Lucene index would be prohibitively expensive.
  */
final class LuceneSchema(sparkFields : StructType, luceneFields : Array[LuceneField]) extends Serializable {
  if (sparkFields.size != luceneFields.length) {
    throw new IllegalArgumentException("Schema size mismatch")
  }

  /**
    * @return the number of fields in this schema
    */
  def size: Int = sparkFields.size

  def sparkFieldType(i : Int) : DataType = sparkField(i).dataType

  def luceneFieldType(i : Int) : FieldType = luceneFields(i).fieldType

  def sparkField(i : Int) : StructField = sparkFields.fields(i)

  def sparkField(name: String) : StructField = sparkFields.fields(sparkFields.fieldIndex(name))

  def luceneField(i : Int) : LuceneField = luceneFields(i)

  def luceneField(name: String) : LuceneField = LuceneSchema.findByName(name, luceneFields)

  /**
    * @return String representation of the Spark schema
    */
  def sparkSchemaStr() : String = sparkFields.json

  /**
    * @return String representation of the Lucene schema
    */
  def luceneSchemaStr() : String = LuceneField.toString(luceneFields)

  /**
    * @return Spark schema
    */
  def sparkSchema() : StructType = sparkFields

  /**
    * @return Lucene schema
    */
  def luceneSchema() : Array[LuceneField] = luceneFields

  /**
    * See org.apache.spark.sql.sources.BaseRelation::unhandledFilters
    * @param filters requested filters
    * @return the requested filters subset that cannot be applied by Lucene-based storage because either the filter type
    *         is not supported or the corresponding Lucene field is not indexed
    */
  def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter((filter: Filter) => !isAttributeIndexed(filter))
  }

  private def isAttributeIndexed(attr: String) : Boolean = {
    luceneFieldType(sparkFields.fieldIndex(attr)) != FieldType.STORED
  }

  private def isAttributeIndexed(filter: Filter) : Boolean = {
    filter match {
      case EqualTo(attr, _) => isAttributeIndexed(attr)

      case GreaterThan(attr, _) => isAttributeIndexed(attr)

      case LessThan(attr, _) => isAttributeIndexed(attr)

      case GreaterThanOrEqual(attr, _) => isAttributeIndexed(attr)

      case LessThanOrEqual(attr, _) => isAttributeIndexed(attr)

      case _ => false
    }
  }
}

/**
  * For every Lucene field remember if it is Indexed (and so can be filtered on), Stored (and so can be retrieved), or
  * both.
  */
sealed trait FieldType {def strFlag: String; }
object FieldType {
  case object INDEXED extends FieldType {val strFlag = "I"}
  case object STORED extends FieldType {val strFlag = "S"}
  case object QUERYABLE extends FieldType {val strFlag = "Q"}

  def apply(str : String) : FieldType = {
    str match {
      case INDEXED.strFlag => INDEXED;
      case STORED.strFlag => STORED;
      case QUERYABLE.strFlag => QUERYABLE;
    }
  }
}

case class LuceneField(name: String, fieldType: FieldType)

object LuceneField {
  val DELIMITER : String = "|"
  val COMMA : String = ":"

  def toString(fields: Array[LuceneField]) : String = {
    fields.map(field => field.name + COMMA + field.fieldType.strFlag).mkString(DELIMITER)
  }

  def fromString(fieldFlags : String) : Array[LuceneField] = {
    fieldFlags.split(DELIMITER.charAt(0)).map(entry => {val kv = entry.split(COMMA); LuceneField(kv(0), FieldType(kv(1)))})
  }
}

object LuceneSchema {
  val SCHEMA_FILE_NAME = "schema.txt"

  def apply(schema : StructType, fieldFlags : String) : LuceneSchema = {
    new LuceneSchema(schema, LuceneField.fromString(fieldFlags))
  }

  // When pruning Spark columns make sure the Lucene field types are aligned with the remaining columns.
  // Notice that "SELECT *" and "df.count()" are represented by an empty pruned schema.
  def apply(requiredSparkSchema : StructType, originalSchema: LuceneSchema) : LuceneSchema = {
    if ((requiredSparkSchema.size == originalSchema.size) || (requiredSparkSchema.isEmpty)) {
      originalSchema
    } else {
      val prunedSparkSchema = deduplicate(requiredSparkSchema)
      val prunedLuceneSchema = Array.ofDim[LuceneField](prunedSparkSchema.size)

      var i = 0
      for (field <- originalSchema.sparkSchema().fields) {
        if (contains(field.name, prunedSparkSchema.fields)) {
          prunedLuceneSchema(i) = findByName(field.name, originalSchema.luceneSchema())
        }
        i += 1
      }

      new LuceneSchema(prunedSparkSchema, prunedLuceneSchema)
    }
  }

  private def contains(fieldName: String, schema: Array[StructField]): Boolean = {
    for (candidate <- schema) {
      if (fieldName == candidate.name) {
        return true
      }
    }

    false
  }

  private def findByName(fieldName: String, schema: Array[LuceneField]): LuceneField = {
    for (candidate <- schema) {
      if (fieldName == candidate.name) {
        return candidate
      }
    }

    throw new IllegalArgumentException(s"Field not found: $fieldName")
  }

  // OR clauses seem to duplicate columns received from SupportsPushDownRequiredColumns::pruneColumns
  // todo is it a Spark bug/feature or I somehow cause it?
  private def deduplicate(schema: StructType) : StructType = {
    val unique = mutable.Set[String]()

    val deduplicated = schema.fields.filter((field: StructField) => {
      if (unique.contains(field.name))
        false
      else {
        unique += field.name
        true
      }
    })

    StructType(deduplicated)
  }

  /**
    * Write a schema to a text file as two lines
    * @param schema schema to write
    * @param filePath destination file path
    */
  def save(schema : LuceneSchema, filePath : String) : Unit = {
    val writer = new FileWriter(filePath)
    try {
      writer.write(schema.luceneSchemaStr())
      writer.write("\n")
      writer.write(schema.sparkSchemaStr())
    } finally {
      writer.close()
    }
  }

  private def create(luceneSchemaStr: String, sparkSchemaStr: String) : LuceneSchema = {
    val sparkSchema = DataType.fromJson(sparkSchemaStr) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing StructType: $sparkSchemaStr")
    }

    apply(sparkSchema, luceneSchemaStr)
  }

  /**
    * @param filePath schema file path
    * @return schema loaded from the given file
    */
  def open(filePath : String) : LuceneSchema = {
    val lines = Source.fromFile(filePath).getLines()

    if (lines.hasNext) {
      val luceneSchemaStr = lines.next()
      if (lines.hasNext) {
        val sparkSchemaStr = lines.next()
        return create(luceneSchemaStr, sparkSchemaStr)
      }
    }

    throw new IllegalArgumentException("Invalid schema file: " + filePath)
  }
}
