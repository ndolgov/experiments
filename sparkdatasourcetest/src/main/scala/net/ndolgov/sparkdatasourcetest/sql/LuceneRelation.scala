package net.ndolgov.sparkdatasourcetest.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SaveMode, SQLContext}

/**
  * Searchable Lucene-based data storage of some application-specific scope (e.g. corresponding to a particular version of
  * a tenant's dataset; the client would be responsible for mapping (tenantId,version) pair to a relationDir)
  *
  * @param relationDir root location for all storage partitions
  * @param userSchema user-provided schema
  * @param sqlContext Spark context
  */
class LuceneRelation(relationDir: String, userSchema: LuceneSchema = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation {

  private val rddSchema : LuceneSchema = getSchema

  private val rdd : LuceneRDD = LuceneRDD(sqlContext.sparkContext, relationDir, rddSchema)

  private def getSchema: LuceneSchema = {
    if (userSchema == null) {
      LuceneSchema.open(LuceneRDD.schemaFilePath(relationDir).toUri.toString)
    } else {
      userSchema
    }
  }

  override def schema: StructType = rddSchema.sparkSchema()

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = rddSchema.unhandledFilters(filters)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    rdd.pruneAndFilter(requiredColumns, filters)
  }

  override def insert(df: Dataset[Row], overwrite: Boolean): Unit = {
    df.
      write.
      format(LuceneDataSource.SHORT_NAME).
      option(LuceneDataSource.PATH, relationDir).
      mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append).
      save()
  }
}

