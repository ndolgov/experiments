package net.ndolgov.sparkdatasourcetest.connector

import net.ndolgov.sparkdatasourcetest.lucene.{LuceneIndexReader, LuceneSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataSourceV2Reader, ReadTask, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType

private final class LuceneDataSourceV2Reader(path: String) extends DataSourceV2Reader with SupportsPushDownFilters {
  private lazy val schema = LuceneSchema.open(FilePaths.schemaFilePath(path).toUri.toString)

  override def readSchema(): StructType = schema.sparkSchema()

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    import scala.collection.JavaConverters._

    FileUtils.listSubDirs(path).map((partitionDir: String) => {
      val task: ReadTask[Row] = new LuceneReadTask(partitionDir, schema)
      task
    }).toList.asJava
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = filters

  override def pushedFilters(): Array[Filter] = Array()
}

private final class LuceneReadTask(partitionDirStr : String, schema: LuceneSchema) extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] = new DataReader[Row] {
    private val rows: Iterator[Row] = LuceneIndexReader(partitionDirStr, schema).iterator

    override def next(): Boolean = rows.hasNext

    override def get(): Row = rows.next()

    override def close(): Unit = {} //todo implement when LIR is a true iterator
  }
}

object LuceneDataSourceV2Reader {
  def apply(path: String) : DataSourceV2Reader = new LuceneDataSourceV2Reader(path)
}
