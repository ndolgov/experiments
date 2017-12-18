package net.ndolgov.sparkdatasourcetest.connector

import java.util

import LuceneDataSourceTestEnv.{DocumentField, createTestDataFrameRows, defaultSchema, sparkSession}
import net.ndolgov.sparkdatasourcetest.connector.LuceneDataSourceV2.{LUCENE_SCHEMA, PATH, SHORT_NAME}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.scalatest.{Assertions, FlatSpec}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Create a dataset with the schema | metric:long | time:long | value:double | where metric is indexed and stored,
  * time is stored only, and value is stored only. Write the dataset to Lucene data source and read it back using a
  * few different query syntax flavors.
  */
final class LuceneDataSourceTestSuit extends FlatSpec with Assertions {
  private val logger : Logger = LoggerFactory.getLogger(this.getClass)
  private val TABLE_NAME: String = "MTV_TABLE"

  "A Row set written to Lucene index" should "be read back with filters applied (and columns potentially re-ordered)" in {
    testWriteReadCycle()
  }

  private def testWriteReadCycle(): Unit = {
    val session: SparkSession = sparkSession("LuceneDataSourceV2TestSuite")

    try {
      val path = "target/test" + System.currentTimeMillis()
      writeToDataSource(session, path)
      readFromDataSource(session, path)
    } finally {
      session.stop
    }
  }

  private def writeToDataSource(session: SparkSession, path : String): Unit = {
    val schema : StructType = defaultSchema
    val now: Long = System.currentTimeMillis
    val rows: util.List[Row] = createTestDataFrameRows(now)

    val df = session.createDataFrame(rows, schema).repartition(LuceneDataSourceTestEnv.PARTITION_NUMBER)

    logger.info("Building index in directory: " + path)

    df.write.
      format(SHORT_NAME).
      option(PATH, path).
      option(LUCENE_SCHEMA, "Q|S|S").
      mode(SaveMode.Overwrite).
      save()
  }

  private def readFromDataSource(session: SparkSession, path : String): Unit = {
    val loaded = session.read.
      format(SHORT_NAME).
      option(PATH, path).
      load()

    loaded.createOrReplaceTempView(TABLE_NAME)

    val count: Long = loaded.count()
    assert(count == LuceneDataSourceTestEnv.ROW_NUMBER)
    logger.info("Registered table with total rows: " + count)

    val loadedWithSql = session.sql("SELECT metric, time, value FROM MTV_TABLE WHERE metric = 3660152")
    assert(loadedWithSql.count() == 10)
    loadedWithSql.show

    val loadedWithSql2 = session.sql("SELECT * FROM MTV_TABLE WHERE metric = 3660153")
    assert(loadedWithSql2.count() == 10)
    loadedWithSql2.show

    val table: Dataset[Row] = session.table(TABLE_NAME)
    val loadedWithSql3 = table.filter(table(DocumentField.METRIC.name).equalTo(3660154))
    assert(loadedWithSql3.count() == 10)
    loadedWithSql3.show

    val loadedWithSql4 = loaded.filter(loaded(DocumentField.METRIC.name).equalTo(3660155))
    assert(loadedWithSql4.count() == 10)
    loadedWithSql4.show

    val loadedWithSql5 = session.sql("SELECT time, metric FROM MTV_TABLE WHERE metric = 3660155")
    assert(loadedWithSql5.count() == 10)
    loadedWithSql5.show
  }

  private def insertIntoDataSource(session: SparkSession, path : String): Unit = {
    val schema : StructType = defaultSchema
    val now: Long = System.currentTimeMillis
    val rows: util.List[Row] = createTestDataFrameRows(now)

    val df = session.createDataFrame(rows, schema)

    logger.info("Building index in directory: " + path)

    df.write.
      format(SHORT_NAME).
      option(PATH, path).
      option(LUCENE_SCHEMA, "Q|S|S").
      mode(SaveMode.Overwrite).
      save()
  }

}


