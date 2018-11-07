package net.ndolgov.sparkdatasourcetest.connector

import net.ndolgov.sparkdatasourcetest.lucene.{LuceneIndexWriter, LuceneSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}

/** Lucene data source write path */
private final class LuceneDataSourceV2Writer(path: String, schema: LuceneSchema) extends DataSourceWriter {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    FileUtils.mkDir(path)
    new LuceneDataWriterFactory(path, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // the same schema for all the partitions
    LuceneSchema.save(schema, FilePaths.schemaFilePath(path).toUri.toString)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // todo delete partition dirs?
  }
}

private final class LuceneDataWriterFactory(rddDir: String, schema: LuceneSchema) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] =
    new LuceneDataWriter(FilePaths.partitionDir(rddDir, partitionId), schema) //todo taskId in file paths
}

private final class LuceneDataWriter(partitionDir: String, schema: LuceneSchema) extends DataWriter[InternalRow] {
  private val rddDir = FileUtils.mkDir(partitionDir)

  private val writer = LuceneIndexWriter(partitionDir, schema)

  override def write(row: InternalRow): Unit = writer.write(row)

  override def commit(): WriterCommitMessage = {
    close()
    LuceneWriterCommitMessage()
  }

  override def abort(): Unit = {
    close()
    FileUtils.deleteRecursively(rddDir)
  }

  private def close(): Unit = writer.close()
}

case class LuceneWriterCommitMessage() extends WriterCommitMessage

object LuceneDataSourceV2Writer {
  def apply(path: String, schema: LuceneSchema) : DataSourceWriter = new LuceneDataSourceV2Writer(path, schema)
}
