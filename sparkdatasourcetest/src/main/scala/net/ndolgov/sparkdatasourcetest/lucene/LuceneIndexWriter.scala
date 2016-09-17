package net.ndolgov.sparkdatasourcetest.lucene

import java.io.File

import net.ndolgov.sparkdatasourcetest.sql.LuceneSchema
import org.apache.hadoop.fs.Path
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, MMapDirectory}
import org.apache.spark.Logging
import org.apache.spark.sql.Row

/**
  * Write a sequence of Rows conforming to a provided schema into a new Lucene index at a given location
  */
object LuceneIndexWriter extends Logging {
  def writeIndex(schema : LuceneSchema, partition: Iterator[Row], indexPath: Path): Unit = {
    writeIndex(schema, partition, indexPath.toUri.toString)
  }

  private def writeIndex(schema : LuceneSchema, partition: Iterator[Row], indexDir: String): Unit = {
    createIndex(indexDir, (indexWriter: IndexWriter) => {
      val writer = LuceneDocumentWriter(schema, indexWriter)

      var rowCount: Int = 0
      partition.foreach((row: Row) => {
        writer.write(row)
        rowCount += 1
      })
      rowCount
    })
  }

  private def createIndex(indexDir: String, dataGenerator: (IndexWriter => Int)): Unit = {
    logInfo("Creating Lucene index in: " + indexDir)

    val directory: Directory = new MMapDirectory(new File(indexDir).toPath)
    val indexWriter = new IndexWriter(directory, new IndexWriterConfig(new KeywordAnalyzer))

    try {
      val count = dataGenerator(indexWriter)

      logInfo("Finished Lucene index creation after processing DataFrame rows: " + count)
    } finally {
      try {
        indexWriter.close()
      } catch {
        case e: Exception => logWarning("Could not close index writer")
      }

      try {
        directory.close()
      } catch {
        case e: Exception => logWarning("Could not close index directory")
      }
    }

  }
}

/**
  * Write a new fixed-schema Lucene document for every given row
  */
private final class LuceneDocumentWriter(indexWriter : IndexWriter, fieldWriter : LuceneFieldWriter, document : Document) {
  def write(row: Row) : Unit = {
    document.clear()

    fieldWriter.write(row)
    indexWriter.addDocument(document)
  }
}

private object LuceneDocumentWriter {
  def apply(schema : LuceneSchema, indexWriter : IndexWriter) : LuceneDocumentWriter = {
    val document = new Document()
    val fieldWriter = LuceneFieldWriter(schema, document)

    new LuceneDocumentWriter(indexWriter, fieldWriter, document)
  }
}
