package net.ndolgov.sparkdatasourcetest.lucene

import java.io.File
import java.nio.file.FileSystems

import org.apache.hadoop.fs.Path
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Read Rows from a Lucene index at a given location into an Array
  */
object LuceneIndexReader {
  private val logger : Logger = LoggerFactory.getLogger(LuceneIndexReader.getClass)
  private val MIN_ALLOWED = 1
  private val fs = FileSystems.getDefault

  def apply(indexDir : String, schema: LuceneSchema) : Array[Row] = {
    apply(indexDir, schema, storedColumns(schema), Array.empty)
  }

  // ignore fields that are not stored
  private def storedColumns(schema: LuceneSchema): ArrayBuffer[String] = {
    val columns = ArrayBuffer[String]()

    for (index <- 0 until schema.size) {
      if (schema.luceneFieldType(index) != FieldType.INDEXED) {
        columns += schema.sparkField(index).name
      }
    }

    columns
  }

  def apply(indexDir : String, schema: LuceneSchema, columns: Seq[String], filters: Array[Filter]) : Array[Row] = {
    val reader : LuceneDocumentReader = LuceneDocumentReader(columns, filters, schema)
    val query = new StoredFieldVisitorQuery(QueryBuilder(filters, schema), reader)

    apply(indexDir, (searcher: IndexSearcher) => {
      searcher.search(query, MIN_ALLOWED)
    })

    reader.retrievedRows()
  }

  def count(indexPath : Path) : Long = {
    val reader: DirectoryReader = DirectoryReader.open(new MMapDirectory(new File(indexPath.toUri.toString).toPath))

    try {
      reader.numDocs()
    } finally {
      close(reader)
    }
  }

  private def apply(indexDir: String, docCollector : (IndexSearcher => Unit)) : Unit = {
    logger.info("Searching dir       : " + indexDir)

    val reader: DirectoryReader = DirectoryReader.open(new MMapDirectory(fs.getPath(indexDir)))
    try {
      docCollector.apply(new IndexSearcher(reader))
    } finally {
      close(reader)
    }
  }

  private def close(reader: DirectoryReader): Unit = {
    val directory = reader.directory()
    try {
      reader.close()
    } catch {
      case e: Exception => logger.warn("Could not close index reader")
    }

    try {
      directory.close()
    } catch {
      case e: Exception => logger.warn("Could not close directory")
    }
  }
}
