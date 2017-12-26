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
      option(LUCENE_SCHEMA, "metric:Q|time:S|value:S").
      mode(SaveMode.Overwrite).
      save()
  }

  private def readFromDataSource(session: SparkSession, path : String): Unit = {
    val metric152 = 3660152
    val metric153 = 3660153
    val metric154 = 3660154
    val metric155 = 3660155
    val metric160 = 3660160
    val metric161 = 3660161

    val loaded = session.read.
      format(SHORT_NAME).
      option(PATH, path).
      load()

    loaded.createOrReplaceTempView(TABLE_NAME)

    val count: Long = loaded.count()
    assert(count == LuceneDataSourceTestEnv.ROW_NUMBER)
    logger.info("Registered table with total rows: " + count)

    val loadedWithSql1 = session.sql(s"SELECT metric, time, value FROM MTV_TABLE WHERE metric = $metric152")
    assert(loadedWithSql1.count() == 10)
    assert(loadedWithSql1.schema.size == 3)
    loadedWithSql1.collect().foreach((row: Row) => assert(row.getLong(0) == metric152))
    loadedWithSql1.show

    val loadedWithSql2 = session.sql(s"SELECT * FROM MTV_TABLE WHERE metric = $metric153")
    assert(loadedWithSql2.count() == 10)
    assert(loadedWithSql2.schema.size == 3)
    loadedWithSql2.collect().foreach((row: Row) => assert(row.getLong(0) == metric153))
    loadedWithSql2.show

    val table: Dataset[Row] = session.table(TABLE_NAME)
    val loadedWithSql3 = table.filter(table(DocumentField.METRIC.name).equalTo(metric154))
    assert(loadedWithSql3.count() == 10)
    assert(loadedWithSql3.schema.size == 3)
    loadedWithSql3.collect().foreach((row: Row) => assert(row.getLong(0) == metric154))
    loadedWithSql3.show

    val loadedWithSql4 = loaded.filter(loaded(DocumentField.METRIC.name).equalTo(metric155))
    assert(loadedWithSql4.count() == 10)
    assert(loadedWithSql4.schema.size == 3)
    loadedWithSql4.collect().foreach((row: Row) => assert(row.getLong(0) == metric155))
    loadedWithSql4.show

    val loadedWithSql5 = session.sql(s"SELECT time, metric FROM MTV_TABLE WHERE metric = $metric155")
    assert(loadedWithSql5.count() == 10)
    assert(loadedWithSql5.schema.size == 2)
    loadedWithSql5.collect().foreach((row: Row) => assert(row.getLong(1) == metric155))
    loadedWithSql5.show

    val loadedWithSql6 = session.sql(s"SELECT * FROM MTV_TABLE WHERE metric < $metric153")
    loadedWithSql6.show
    assert(loadedWithSql6.count() == 10)
    assert(loadedWithSql6.schema.size == 3)
    loadedWithSql6.collect().foreach((row: Row) => assert(row.getLong(0) == metric152))

    val loadedWithSql7 = session.sql(s"SELECT * FROM MTV_TABLE WHERE metric <= $metric153")
    loadedWithSql7.show
    assert(loadedWithSql7.count() == 20)
    assert(loadedWithSql7.schema.size == 3)
    loadedWithSql7.collect().foreach((row: Row) => assert((row.getLong(0) == metric152) || (row.getLong(0) == metric153)))

    val loadedWithSql8 = session.sql(s"SELECT * FROM MTV_TABLE WHERE metric >= $metric160")
    loadedWithSql8.show
    assert(loadedWithSql8.count() == 20)
    assert(loadedWithSql8.schema.size == 3)
    loadedWithSql8.collect().foreach((row: Row) => assert((row.getLong(0) == metric160) || (row.getLong(0) == metric161)))

    val loadedWithSql9 = session.sql(s"SELECT * FROM MTV_TABLE WHERE metric > $metric160")
    loadedWithSql9.show
    assert(loadedWithSql9.count() == 10)
    assert(loadedWithSql9.schema.size == 3)
    loadedWithSql9.collect().foreach((row: Row) => assert(row.getLong(0) == metric161))

    val loadedWithSql10 = session.sql(s"SELECT metric, time, value FROM MTV_TABLE WHERE metric < $metric153 OR metric > $metric160")
    loadedWithSql10.show
    assert(loadedWithSql10.count() == 20)
    assert(loadedWithSql10.schema.size == 3)
    loadedWithSql10.collect().foreach((row: Row) => assert((row.getLong(0) == metric152) || (row.getLong(0) == metric161)))
  }

}


