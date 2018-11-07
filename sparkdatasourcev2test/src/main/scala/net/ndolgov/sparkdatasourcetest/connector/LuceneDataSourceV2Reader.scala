package net.ndolgov.sparkdatasourcetest.connector

import net.ndolgov.sparkdatasourcetest.lucene.{LuceneIndexReader, LuceneSchema, QueryBuilder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

/** Lucene data source read path */
private final class LuceneDataSourceV2Reader(path: String)
  extends DataSourceReader with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private lazy val schema = LuceneSchema.open(FilePaths.schemaFilePath(path).toUri.toString)

  private var prunedSchema: Option[LuceneSchema] = None

  private var pushedPredicates: Array[Filter] = Array()

  override def readSchema(): StructType = prunedSchema.getOrElse(schema).sparkSchema()

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._

    FileUtils.listSubDirs(path).map((partitionDir: String) => {
      val task: InputPartition[InternalRow] = new LuceneReadTask(partitionDir, prunedSchema.getOrElse(schema), pushedPredicates)
      task
    }).toList.asJava
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(LuceneSchema(requiredSchema, schema))
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = QueryBuilder.distinguishSupported(filters)
    pushedPredicates = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = pushedPredicates
}

private final class LuceneReadTask(partitionDirStr : String, schema: LuceneSchema, filters: Array[Filter]) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new InputPartitionReader[InternalRow] {
    private val rows: Iterator[InternalRow] = LuceneIndexReader(partitionDirStr, schema, filters).iterator

    override def next(): Boolean = rows.hasNext

    override def get(): InternalRow = rows.next()

    override def close(): Unit = {} //todo implement when LIR is a true iterator
  }
}

object LuceneDataSourceV2Reader {
  def apply(path: String) : DataSourceReader = new LuceneDataSourceV2Reader(path)
}
