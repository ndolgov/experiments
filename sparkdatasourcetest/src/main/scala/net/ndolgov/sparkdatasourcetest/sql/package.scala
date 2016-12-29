package net.ndolgov.sparkdatasourcetest

import org.apache.spark.sql.{Dataset, Row, SQLContext}

/**
  * Extend Spark API with LuceneRDD support
  */
package object sql {

  /**
    * Extend SQLContext API
    */
  implicit class LuceneSqlContext(sqlContext : SQLContext) {
    /**
      * Load data from the Lucene-based storage at a given location to a data frame
      * @param path data storage location
      */
    def luceneTable(path: String): Unit = {
      sqlContext.baseRelationToDataFrame(new LuceneRelation(path)(sqlContext))
    }
  }

  /**
    * Extend DataFrame API
    */
  implicit class LuceneDataFrame(df: Dataset[Row]) {
    /**
      * Save a data frame to the Lucene-based storage
      * @param path storage location for the given dataset
      * @param luceneSchema data schema
      */
    def saveAsLuceneIndex(path : String, luceneSchema : String): Unit = {
      LuceneRDD(df, luceneSchema).save(path)
    }

    /**
      * @return the number of rows in the data frame counted in a more efficient way than the default one (that requires
      *         the entire dataset to be moved to the driver before counting)
      */
    def countRows(): Long = {
      LuceneRDD(df).count()
    }
  }
}