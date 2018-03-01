package net.ndolgov.sparkdatasourcetest.connector

import java.util.Optional

import net.ndolgov.sparkdatasourcetest.lucene.LuceneSchema
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceOptions, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

final class LuceneDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport with DataSourceRegister {
  import LuceneDataSourceV2._

  override def createReader(options: DataSourceOptions): DataSourceReader =
    LuceneDataSourceV2Reader(path(options))

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] =
    Optional.of(
      LuceneDataSourceV2Writer(
        path(options),
        LuceneSchema(schema, luceneSchema(options))))

  override def shortName(): String = SHORT_NAME
}

object LuceneDataSourceV2 {
  /** alternative/long format name for when "META-INF.services" trick is not used */
  val FORMAT : String = "net.ndolgov.sparkdatasourcev2test.sql"

  /** The default/short format name, usage example: "df.write.format(LuceneDataSource.SHORT_NAME)" */
  val SHORT_NAME : String = "LuceneDataSourceV2"

  /** The root directory (presumably in a DFS) for Lucene data storage item. IRL would be based on tenantId/data version/etc */
  val PATH : String = "path"

  /** The Lucene schema that describes which columns are indexed and/or stored */
  val LUCENE_SCHEMA : String = "lucene.schema"

  private def path(options: DataSourceOptions): String = {
    stringArg(PATH, options)
  }

  private def luceneSchema(options: DataSourceOptions): String = {
    stringArg(LUCENE_SCHEMA, options)
  }

  private def stringArg(key : String, options: DataSourceOptions): String = {
    val mayBy = options.get(key)
    if (mayBy.isPresent) mayBy.get() else throw new IllegalArgumentException("Option is missing: " + key)
  }
}

