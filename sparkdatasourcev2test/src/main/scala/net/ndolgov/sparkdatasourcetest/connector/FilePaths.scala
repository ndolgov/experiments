package net.ndolgov.sparkdatasourcetest.connector

import net.ndolgov.sparkdatasourcetest.lucene.LuceneSchema
import org.apache.hadoop.fs.Path

object FilePaths {
  private val PART : String = "/part-"

  /**
    * @param rddDir RDD root path
    * @param index RDD partition index
    * @return RDD partition path
    */
  def partitionPath(rddDir: String, index : Long) : Path = new Path(partitionDir(rddDir, index))

  /**
    * @param rddDir RDD root path
    * @param index RDD partition index
    * @return RDD partition path
    */
  def partitionDir(rddDir: String, index : Long) : String = rddDir + PART + "%05d".format(index)

  /**
    * @param rddDir RDD root path
    * @return schema file path
    */
  def schemaFilePath(rddDir: String) : Path = new Path(rddDir + "/" + LuceneSchema.SCHEMA_FILE_NAME)
}
