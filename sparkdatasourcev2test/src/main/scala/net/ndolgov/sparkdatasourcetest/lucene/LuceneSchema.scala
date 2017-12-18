package net.ndolgov.sparkdatasourcetest.lucene

import java.io.FileWriter

import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, LessThan}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.io.Source

/**
  * The data schema used by the connector and comprised of a Spark schema (fields types) and a Lucene schema
  * (can a field be retrieved or filtered on?). The latter is necessary because not all Lucene documents are required
  * to have all the fields and so reverse engineering the schema from a Lucene index would be prohibitively expensive.
  */
final class LuceneSchema(schema : StructType, fieldTypes : Array[FieldType]) extends Serializable {
  if (schema.size != fieldTypes.length) {
    throw new IllegalArgumentException("Schema size mismatch")
  }

  /**
    * @return the number of fields in this schema
    */
  def size = schema.size

  def sparkFieldType(i : Int) : DataType = sparkField(i).dataType

  def luceneFieldType(i : Int) : FieldType = fieldTypes(i)

  def sparkField(i : Int) : StructField = schema.fields(i)

  /**
    * @return String representation of the Spark schema
    */
  def sparkSchemaStr() : String = schema.json

  /**
    * @return String representation of the Lucene schema
    */
  def luceneSchemaStr() : String = FieldType.toString(fieldTypes)

  /**
    * @return Spark schema
    */
  def sparkSchema() : StructType = schema

  /**
    * @return Lucene schema
    */
  def luceneSchema() : Array[FieldType] = fieldTypes

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
    luceneFieldType(schema.fieldIndex(attr)) != FieldType.STORED
  }

  private def isAttributeIndexed(filter: Filter) : Boolean = {
    filter match {
      case EqualTo(attr, value) => isAttributeIndexed(attr)

      case GreaterThan(attr, value) => isAttributeIndexed(attr)

      case LessThan(attr, value) => isAttributeIndexed(attr)

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

  val DELIMITER : String = "|"

  def apply(str : String) : FieldType = {
    str match {
      case INDEXED.strFlag => INDEXED;
      case STORED.strFlag => STORED;
      case QUERYABLE.strFlag => QUERYABLE;
    }
  }

  def toString(fieldTypes : Array[FieldType]) : String = {
    fieldTypes.map(field => field.strFlag).mkString(DELIMITER)
  }

  def fromString(fieldFlags : String) : Array[FieldType] = {
    fieldFlags.split(FieldType.DELIMITER.charAt(0)).map(flag => FieldType(flag))
  }
}

object LuceneSchema {
  val SCHEMA_FILE_NAME = "schema.txt"

  def apply(schema : StructType, fieldFlags : String) : LuceneSchema = {
    new LuceneSchema(schema, FieldType.fromString(fieldFlags))
  }

  // unusual case when Lucene schema is unknown and unnecessary let's pretend all the fields are stored
  def apply(schema : StructType) : LuceneSchema = {
    new LuceneSchema(schema, schema.fields.map(field => FieldType.STORED))
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
