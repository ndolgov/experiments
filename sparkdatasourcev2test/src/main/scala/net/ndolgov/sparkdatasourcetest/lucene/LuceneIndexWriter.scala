package net.ndolgov.sparkdatasourcetest.lucene

import java.io.File

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, MMapDirectory}
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

/**
  * Write a sequence of Rows conforming to a provided schema into a new Lucene index at a given location
  */
object LuceneIndexWriter {
  val logger: Logger = LoggerFactory.getLogger(LuceneIndexWriter.getClass)

  def apply(indexDir: String, schema: LuceneSchema): LuceneIndexWriter = {
    logger.info("Creating Lucene index in: " + indexDir)

    val directory: Directory = new MMapDirectory(new File(indexDir).toPath)
    val indexWriter = new IndexWriter(directory, new IndexWriterConfig(new KeywordAnalyzer))

    val document = new Document()
    val fieldWriter = LuceneFieldWriter(schema, document)

    new LuceneIndexWriterImpl(indexWriter, fieldWriter, directory, document)
  }
}

trait LuceneIndexWriter {
  def write(row: Row)

  def close()
}

/**
  * Write a new fixed-schema Lucene document for every given row
  */
private final class LuceneIndexWriterImpl(indexWriter : IndexWriter,
                                          fieldWriter : LuceneFieldWriter,
                                          directory: Directory,
                                          document : Document) extends LuceneIndexWriter {
  import LuceneIndexWriter.logger

  def write(row: Row) : Unit = {
    document.clear()

    fieldWriter.write(row)
    indexWriter.addDocument(document)
  }

  override def close(): Unit = {
    try {
      indexWriter.close()
    } catch {
      case _: Exception => logger.warn("Could not close index writer")
    }

    try {
      directory.close()
    } catch {
      case _: Exception => logger.warn("Could not close index directory")
    }

  }
}
