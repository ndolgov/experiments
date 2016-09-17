package net.ndolgov.sparkdatasourcetest.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, Row}

/**
  * A facade for multiple storage partitions. Every operation is delegated to all the partitions.
  */
final class LuceneRDD(val partitionsRDD: RDD[LucenePartition], val schema: LuceneSchema)
  extends RDD[Row](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partitionIterator = firstParent[LucenePartition].iterator(split, context)
    if (partitionIterator.hasNext) {
      partitionIterator.next().iterator
    } else {
      Iterator[Row]()
    }
  }

  override def count(): Long = partitionsRDD.map(_.count()).aggregate(0L)(_ + _,  _ + _)

  /**
    * Save the [[LucenePartition]]s to disk by writing them to Lucene index
    *
    * @param rddDir root path shared by all Lucene index paths of this RDD
    */
  def save(rddDir: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(new Path(rddDir.stripSuffix("/")).toUri, conf)
    fs.mkdirs(new Path(rddDir))

    // the same schema for all the partitions
    LuceneSchema.save(schema, LuceneRDD.schemaFilePath(rddDir).toUri.toString)

    partitionsRDD.zipWithIndex().foreach(entry => {
      val partitionDir = LuceneRDD.partitionPath(rddDir, entry._2)

      val fs = FileSystem.get(partitionDir.toUri, new Configuration())
      fs.mkdirs(partitionDir)

      //val os = fs.create(partitionDir)
      LucenePartition.save(partitionDir, entry._1, schema)
      //os.close()
    })

  }

  def pruneAndFilter(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    partitionsRDD.flatMap(partition => partition.search(requiredColumns, filters))
  }
}

object LuceneRDD {
  private val PART : String = "/part-"

  /**
    * @param rddDir RDD root path
    * @param index RDD partition index
    * @return RDD partition path
    */
  private def partitionPath(rddDir: String, index : Long) : Path = new Path(rddDir + LuceneRDD.PART + "%05d".format(index))

  /**
    * @param rddDir RDD root path
    * @return schema file path
    */
  def schemaFilePath(rddDir: String) : Path = new Path(rddDir + "/" + LuceneSchema.SCHEMA_FILE_NAME)

  /**
    * Load a [[net.ndolgov.sparkdatasourcetest.sql.LuceneRDD LuceneRDD]] from storage
    *
    * @param sc spark context
    * @param rddDir path to read LuceneRDD from
    * @return LuceneRDD
    */
  def apply(sc: SparkContext, rddDir: String, schema: LuceneSchema): LuceneRDD = {
    val rddPath = new Path(rddDir)
    val fs = FileSystem.get(rddPath.toUri, sc.hadoopConfiguration)

    val partitionSubPathStatus = fs.listStatus(rddPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })

    val numPartitions = partitionSubPathStatus.length

    val partitions = sc.parallelize(0 until numPartitions, numPartitions).
      mapPartitionsWithIndex[LucenePartition]((index, partition) => {
      Iterator(LucenePartition(partitionPath(rddDir.stripSuffix("/"), index), schema))
    })

    new LuceneRDD(partitions, schema)
  }

  /**
    * Convert an RDD to a [[net.ndolgov.sparkdatasourcetest.sql.LuceneRDD LuceneRDD]] in the write path
    *
    * @param rdd data frame RDD
    * @param schema data frame schema
    * @return new LuceneRDD
    */
  def apply(rdd: RDD[Row], schema: LuceneSchema): LuceneRDD = {
    val partitions = rdd.mapPartitions {
      partition => Iterator(LucenePartition(partition))
    }
    new LuceneRDD(partitions, schema)
  }

  /**
    * Convert a data frame to a [[net.ndolgov.sparkdatasourcetest.sql.LuceneRDD LuceneRDD]] in the write path
    *
    * @param dataFrame input data frame
    * @return new LuceneRDD
    */
  def apply(dataFrame: DataFrame, luceneSchema : String): LuceneRDD = {
    apply(dataFrame.rdd, LuceneSchema(dataFrame.schema, luceneSchema))
  }

  /**
    * Support countRows() calls that don't require Lucene schema knowledge
    * @param dataFrame data frame to count rows in
    * @return new LuceneRDD
    */
  def apply(dataFrame: DataFrame): LuceneRDD = {
    apply(dataFrame.rdd, LuceneSchema(dataFrame.schema))
  }
}
