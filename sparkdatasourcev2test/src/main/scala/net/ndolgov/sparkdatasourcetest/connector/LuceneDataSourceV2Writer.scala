package net.ndolgov.sparkdatasourcetest.connector

import net.ndolgov.sparkdatasourcetest.lucene.{LuceneIndexWriter, LuceneSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataSourceV2Writer, DataWriter, DataWriterFactory, WriterCommitMessage}

private final class LuceneDataSourceV2Writer(path: String, schema: LuceneSchema) extends DataSourceV2Writer {
  override def createWriterFactory(): DataWriterFactory[Row] = {
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

private final class LuceneDataWriterFactory(rddDir: String, schema: LuceneSchema) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] =
    new LuceneDataWriter(FilePaths.partitionDir(rddDir, partitionId), schema)
}

private final class LuceneDataWriter(partitionDir: String, schema: LuceneSchema) extends DataWriter[Row] {
  private val rddDir = FileUtils.mkDir(partitionDir)

  private val writer = LuceneIndexWriter(partitionDir, schema)

  override def write(row: Row): Unit = writer.write(row)

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
  def apply(path: String, schema: LuceneSchema) : DataSourceV2Writer = new LuceneDataSourceV2Writer(path, schema)
}
