package net.ndolgov.sparkdatasourcetest.connector

import java.io.File
import java.util

import com.google.common.collect.Lists
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

/**
  * Auxiliary routines used in test fixture creation
  */
object LuceneDataSourceTestEnv {
  val ROW_NUMBER: Int = 100
  val PARTITION_NUMBER: Int = 2

  def sparkSession(name : String) : SparkSession = {
    SparkSession.builder().
      appName(name).
      master("local[2]").
      config(SparkCtxCfg.SPARK_EXECUTOR_MEMORY, "1g").
      config(SparkCtxCfg.SPARK_SERIALIZER, SparkCtxCfg.KRYO).
      config(SparkCtxCfg.SPARK_SQL_SHUFFLE_PARTITIONS, "2").
      config(SparkCtxCfg.SPARK_WAREHOUSE_DIR, "target/spark-warehouse").
      config(SparkCtxCfg.SPARK_JARS, SparkCtxCfg.toAbsolutePaths("", "")).
      config(SparkCtxCfg.SPARK_DRIVER_HOST, "localhost").
      config(SparkCtxCfg.SPARK_DRIVER_PORT, "31000").
      getOrCreate()
  }

  def defaultSchema = StructType(
    StructField(DocumentField.METRIC.name, LongType, false) ::
    StructField(DocumentField.TIME.name, LongType, false) ::
    StructField(DocumentField.VALUE.name, DoubleType, false) ::
    Nil)

  def createTestDataFrameRows(now: Long): util.List[Row] = {
    val rows: util.List[Row] = Lists.newArrayListWithCapacity(ROW_NUMBER)

    var i: Int = 0
    while (i < ROW_NUMBER) {
      {
        val metricId: Long = 3660152 + i % 10
        val timestamp: Long = now + i * 100000
        val value: Double = 123456.789 + i * 10
        rows.add(Row(metricId, timestamp, value))
        i += 1
      }
    }
    rows
  }

  /**
    * Test schema fields
    */
  sealed trait DocumentField {def name: String; }
  object DocumentField {
    case object METRIC extends DocumentField {val name = "metric"}
    case object TIME extends DocumentField {val name = "time"}
    case object VALUE extends DocumentField {val name = "value"}
  }
}

object SparkCtxCfg {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"

  val SPARK_SERIALIZER = "spark.serializer"

  val ALLOW_MULTIPLE_CONTEXTS = "spark.driver.allowMultipleContexts"

  val SPARK_JARS = "spark.jars"

  val SPARK_WAREHOUSE_DIR = "spark.sql.warehouse.dir"

  val KRYO = "org.apache.spark.serializer.KryoSerializer"

  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"

  val DEFAULT_SPARK_MASTER_URL = "spark://127.0.0.1:7077"

  val SPARK_DRIVER_HOST = "spark.driver.host"

  val SPARK_DRIVER_PORT = "spark.driver.port"

  def envProperty(name : String, otherwise : String) : String = {
    val prop = System.getProperty(name)
    if (prop == null) otherwise else prop
  }

  def availableProcessors() : String = {
    Integer.toString(Runtime.getRuntime.availableProcessors())
  }

  def toAbsolutePaths(jarsString: String, baseDir: String): String = {
    if (jarsString == null || jarsString.length == 0) {
      return ""
    }
    val libDir: String = if (baseDir.endsWith(File.separator)) baseDir
    else baseDir + File.separator
    toAbsolutePaths(libDir, jarsString.split(",")).mkString(",")
  }

  private def toAbsolutePaths(libDir: String, jarFileNames: Array[String]): Array[String] = {
    jarFileNames.map(jar => libDir + jar)
  }
}
