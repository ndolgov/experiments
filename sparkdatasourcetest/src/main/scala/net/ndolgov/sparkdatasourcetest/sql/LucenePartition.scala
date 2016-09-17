package net.ndolgov.sparkdatasourcetest.sql

import net.ndolgov.sparkdatasourcetest.lucene.{LuceneIndexReader, LuceneIndexWriter}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.Row

/**
  * A subset of original data operated on by Spark as a partition.
  */
final class LucenePartition(storage : LucenePartitionStorage) {
  def iterator: Iterator[Row] = storage.iterator

  def search(columns: Array[String], filters: Array[Filter]) : Iterator[Row] = storage.search(columns, filters)

  def count() : Long = storage.count()
}

/**
  * A means of using different storage in read and write paths. In the read path the storage is an actual Lucene index.
  * In the write path the storage is a temporary in-memory data structure holding data before it's written to Lucene index.
  */
private trait LucenePartitionStorage {
  def iterator: Iterator[Row]

  def search(columns: Array[String], filters: Array[Filter]) : Iterator[Row]

  def count() : Long
}

/**
  * A temporary in-memory data structure holding data before it's written to Lucene index
  * @param data partition data
  */
private final class InMemoryStorage(data: Iterator[Row]) extends LucenePartitionStorage {
  private val rows = data.toBuffer // todo a more efficient way?

  override def iterator: Iterator[Row] = {
    rows.iterator
  }

  override def search(columns: Array[String], filters: Array[Filter]): Iterator[Row] = {
    iterator
  }

  override def count() : Long = rows.size
}

/**
  * A Lucene index representing data from a Spark partition.
  * @param partitionDir Lucene index location
  * @param schema data schema
  */
private final class LuceneIndexStorage(partitionDir : Path, schema: LuceneSchema) extends LucenePartitionStorage {
  override def iterator: Iterator[Row] = {
    LuceneIndexReader(partitionDir, schema).iterator
  }

  override def search(columns: Array[String], filters: Array[Filter]): Iterator[Row] = {
    LuceneIndexReader(partitionDir, schema, columns, filters).iterator
  }

  override def count() : Long = {
    LuceneIndexReader.count(partitionDir)
  }
}

object LucenePartition {
  /**
    * Write partition data to a new Lucene index at the given location
    * @param partitionDir index files location
    * @param partition partition data
    * @param schema data schema
    */
  def save(partitionDir : Path, partition : LucenePartition, schema : LuceneSchema): Unit = {
    LuceneIndexWriter.writeIndex(schema, partition.iterator, partitionDir)
  }

  /**
    * Wrap source data into a Lucene partition instance in the write path
    * @param data partition data
    * @return a new Lucene partition instance representing the given data in memory before it's written to Lucene index
    */
  def apply(data : Iterator[Row]) : LucenePartition = {
    new LucenePartition(new InMemoryStorage(data))
  }

  /**
    * Load a partition from disk
    * @param partitionDir partition location
    * @param schema partition schema
    * @return a new Lucene partition instance representing the data read from Lucene index
    */
  def apply(partitionDir : Path, schema: LuceneSchema) : LucenePartition = {
    new LucenePartition(new LuceneIndexStorage(partitionDir, schema))
  }
}
