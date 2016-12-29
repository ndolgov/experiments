package net.ndolgov.sparkdatasourcetest.sql

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * [[net.ndolgov.sparkdatasourcetest.sql.LuceneRelation LuceneRelation]] factory representing a Lucene-based data storage
  */
final class DefaultSource extends RelationProvider with DataSourceRegister with CreatableRelationProvider with SchemaRelationProvider {

  override def shortName(): String = LuceneDataSource.SHORT_NAME

  /**
    * It is called by "SQLContext.read().load()"
    * @param sqlContext
    * @param parameters options provided by the client
    * @return a relation representing the written data
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new LuceneRelation(path(parameters))(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    new LuceneRelation(path(parameters), LuceneSchema(schema, luceneSchema(parameters)))(sqlContext)
  }

  /**
    * It is called by "DataFrame.write().save()"
    * @param sqlContext context
    * @param mode saving mode
    * @param parameters options provided by the client
    * @param df data frame to write to the data storage represented by this instance
    * @return a relation representing the written data
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: Dataset[Row]): BaseRelation = {
    val relationDir = path(parameters)
    val relationPath = new Path(relationDir)
    val fs = relationPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    val doSave = if (fs.exists(relationPath)) {
      mode match {
        case SaveMode.Append =>
          throw new IllegalArgumentException("Append mode is not supported by: " + this.getClass.getName)

        case SaveMode.ErrorIfExists =>
          throw new IllegalArgumentException("Index path already exists: " + relationPath.toUri)

        case SaveMode.Overwrite =>
          fs.delete(relationPath, true)
          true

        case SaveMode.Ignore =>
          false
      }
    } else {
      true
    }

    if (doSave) {
      df.saveAsLuceneIndex(relationDir, luceneSchema(parameters))
    }

    createRelation(sqlContext, parameters, df.schema)
  }

  private def path(parameters: Map[String, String]): String = {
    stringArg(LuceneDataSource.PATH, parameters)
  }

  private def luceneSchema(parameters: Map[String, String]): String = {
    stringArg(LuceneDataSource.LUCENE_SCHEMA, parameters)
  }

  private def stringArg(key : String, parameters: Map[String, String]): String = {
    parameters.get(key) match {
      case Some(value) => value
      case _ => throw new IllegalArgumentException("Parameter is missing: " + key)
    }
  }
}

object LuceneDataSource {
  /** alternative/long format name for when "META-INF.services" trick is not used */
  val FORMAT : String = "net.ndolgov.sparkdatasourcetest.sql"

  /** The default/short format name, usage example: "df.write.format(LuceneDataSource.SHORT_NAME)" */
  val SHORT_NAME : String = "lucene"

  /** The root directory (presumably in a DFS) for Lucene data storage item. IRL would be based on tenantId/data version/etc */
  val PATH : String = "path"

  /** The Lucene schema that describes which columns are indexed and/or stored */
  val LUCENE_SCHEMA : String = "lucene.schema"
}
